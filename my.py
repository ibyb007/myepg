import requests
import gzip
from io import BytesIO
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import os
import traceback

def fetch_epg(url, max_retries=5):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=90)
            r.raise_for_status()
            content = r.content

            try:
                xml_str = gzip.decompress(content).decode('utf-8')
                print(f"[+] Decompressed → {len(xml_str):,} chars")
                return xml_str
            except:
                xml_str = content.decode('utf-8')
                print(f"[+] Plain XML → {len(xml_str):,} chars")
                return xml_str
        except Exception as e:
            print(f"[!] Attempt {attempt+1}/{max_retries} failed: {e}")
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status is not None and 400 <= status < 500:
                break   # moved / removed / blocked: retrying will not help, go to the next source
            if attempt < max_retries - 1:
                time.sleep(3 * (attempt + 1))
    print(f"[X] Failed to fetch {url}")
    return None

def parse_epg(xml_str):
    if not xml_str or len(xml_str) < 1000:
        raise ValueError("Empty or invalid XML")
    return ET.fromstring(xml_str)

def extract_channels_and_programmes(root, keywords=None, channel_ids=None):
    channels = {}
    programmes = []

    # Channels - exact ID match if channel_ids provided
    for channel in root.findall('.//channel'):
        ch_id = channel.attrib.get('id')
        if not ch_id:
            continue

        if channel_ids:
            if ch_id not in channel_ids:
                continue
        elif keywords:
            display_names = [dn.text.lower() for dn in channel.findall('display-name') if dn.text]
            if not any(any(kw in name for kw in keywords) for name in display_names):
                continue

        # Keep the channel (note: if duplicate IDs exist in source, last one wins)
        channels[ch_id] = channel

    # Programmes (next 8 days only)
    now = datetime.now()
    cutoff = now + timedelta(days=8)
    for prog in root.findall('.//programme'):
        ch_id = prog.attrib.get('channel')
        if ch_id in channels:
            start_str = prog.attrib.get('start', '')
            if len(start_str) >= 14:
                try:
                    start_dt = datetime.strptime(start_str[:14], '%Y%m%d%H%M%S')
                    if start_dt < now - timedelta(days=1):
                        continue
                    if start_dt > cutoff:
                        continue
                except:
                    pass
            programmes.append(prog)

    return channels, programmes

def filter_out_regional(channels_dict):
    exclude = ['tamil', 'telugu', 'malayalam', 'kannada', 'punjabi', 'marathi', 'gujarati', 'oriya', 'bhojpuri', 'urdu']
    filtered = {}
    removed = 0
    for ch_id, ch in channels_dict.items():
        names = [dn.text.lower() for dn in ch.findall('display-name') if dn.text]
        if any(bad in ' '.join(names) for bad in exclude):
            removed += 1
        else:
            filtered[ch_id] = ch
    if removed:
        print(f"    → Removed {removed} regional language channels")
    return filtered

def channel_matches(channel, keyword):
    names = [dn.text.lower() for dn in channel.findall('display-name') if dn.text]
    return any(keyword in n for n in names)

def fetch_with_backups(label, urls, keywords=None, post_filter=None):
    """Try the sources of one group in priority order.

    - A source that is down (or returns an invalid guide) is skipped and the next one is tried.
    - With keywords: a keyword that the first source has no channel for is looked up in the backup
      sources, and only the missing channels are taken from them (nothing already found is replaced).
    - Without keywords: the first source that works is enough.
    """
    channels, programmes = {}, []
    missing = [k.lower() for k in keywords] if keywords else None

    for i, url in enumerate(urls):
        tag = "primary" if i == 0 else f"backup {i}"
        print(f"   [{label}] {tag}: {url}")
        xml = fetch_epg(url)
        if not xml:
            continue
        try:
            root = parse_epg(xml)
        except Exception as e:
            print(f"   [{label}] {tag} unusable: {e}")
            continue

        ch, prog = extract_channels_and_programmes(root, keywords=missing)
        if post_filter:
            ch = post_filter(ch)
            prog = [p for p in prog if p.attrib.get('channel') in ch]

        # only channels not found yet; the first source that had a channel wins
        new_ids = {cid for cid in ch if cid not in channels and cid not in all_channels}
        for cid in new_ids:
            channels[cid] = ch[cid]
        programmes.extend(p for p in prog if p.attrib.get('channel') in new_ids)
        print(f"   [{label}] {tag}: +{len(new_ids)} channels")

        if missing is None:
            if channels:
                break
            continue

        missing = [k for k in missing if not any(channel_matches(c, k) for c in channels.values())]
        if not missing:
            break
        print(f"   [{label}] no channels yet for: {', '.join(missing)} -> trying next source")

    if missing:
        print(f"   [{label}] ⚠️ still missing: {', '.join(missing)}")
    return channels, programmes

# ========================================
# CONFIG
# ========================================

# Every group lists its sources in priority order: the first one is the primary source, the rest are backups.
# A backup is used when the source before it is down, or when it has no guide for some of the group's channels.
GLOBE   = 'https://raw.githubusercontent.com/globetvapp/epg/main'
OPEN    = 'https://www.open-epg.com/files'
IPTVEPG = 'https://iptv-epg.org/files'

UK_EPG_URLS = [
    'https://epg.pw/xmltv/epg_GB.xml.gz',
    f'{IPTVEPG}/epg-gb.xml.gz',
    f'{OPEN}/unitedkingdom1.xml.gz',
    f'{OPEN}/unitedkingdom2.xml.gz',
    f'{OPEN}/unitedkingdom3.xml.gz',
    f'{GLOBE}/Unitedkingdom/unitedkingdom1.xml',
]
IN_EPG_URLS = [                                                  # Jio (Hindi/English)
    'https://avkb.short.gy/epg.xml.gz',
    f'{IPTVEPG}/epg-in.xml.gz',
    f'{OPEN}/india1.xml.gz',
]
IN_EPG_PW_URLS = [                                               # selective channel IDs
    'https://epg.pw/xmltv/epg_IN.xml.gz',
]
AU_EPG_URLS = [
    'https://iptv-epg.org/files/epg-au.xml.gz',
    'https://epg.pw/xmltv/epg_AU.xml.gz',
    f'{OPEN}/australia1.xml.gz',
    f'{OPEN}/australia2.xml.gz',
    f'{GLOBE}/Australia/australia1.xml',
]
US_EPG_URLS = [                                                  # Willow Sports comes from the US guides
    'https://epg.pw/xmltv/epg_US.xml.gz',
    f'{IPTVEPG}/epg-us.xml.gz',
    f'{OPEN}/usa1.xml.gz',
    f'{GLOBE}/Sports/sports1.xml',
]

UK_KEYWORDS    = ['sky', 'tnt sports', 'premier sports', 'bt sport', 'eurosport', 'itv', 'bbc']
AU_KEYWORDS    = ['fox', 'au: kayo 4k', 'au: bein', 'au: espn', 'astro']
US_KEYWORDS    = ['willow']

# All requested channel IDs from epg.pw IN source (unique set)
TARGET_CHANNEL_IDS = {
    '463932',   # Sony Pix hd
    '404001',   # Zee bangla hd
    '464235',   # Star Jalsha HD
    '464213',   # Sony BBC Earth HD
    '464165',   # Star Gold hd
    '407811',   # 9x jalwa
    '463907',   # Zee cinema hd
    '463939',   # Star Gold 2 HD
    '463898',   # Star Gold Select HD / Star Movies Select HD (shared ID)
    '463993',   # Star Movies HD
    '441259',   # Zee TV (SD)
    '441340',   # Zee TV HD
    '464226',   # &TV (SD)
    '492945',   # &TV HD
    '493399',   # Zee Cinema (SD)
    '464142',   # &pictures (SD)
    '463886',   # &pictures HD
    '448070'    # &xplor HD
}

print("=== Starting EPG Merge (UK + IN(Jio) + IN(epg.pw selective) + AU + US Willow, with backup sources) ===\n")

all_channels = {}
all_programmes = []

try:
    # 1. UK Sports
    print("1. Fetching UK EPG...")
    uk_ch, uk_prog = fetch_with_backups("UK", UK_EPG_URLS, keywords=UK_KEYWORDS)
    all_channels.update(uk_ch)
    all_programmes.extend(uk_prog)
    print(f"   → UK: {len(uk_ch)} channels | {len(uk_prog)} programmes\n")

    # 2. India (Jio - Hindi/English only)
    print("2. Fetching India EPG (Jio)...")
    in_ch, in_prog = fetch_with_backups("IN", IN_EPG_URLS, post_filter=filter_out_regional)
    all_channels.update(in_ch)
    all_programmes.extend(in_prog)
    print(f"   → India (Jio filtered): {len(in_ch)} channels | {len(in_prog)} programmes\n")

    # 2.5 India epg.pw - only the requested specific channels
    print(f"2.5 Fetching India EPG (epg.pw) - {len(TARGET_CHANNEL_IDS)} targeted channels...")
    in_pw_xml = None
    for url in IN_EPG_PW_URLS:
        in_pw_xml = fetch_epg(url)
        if in_pw_xml:
            break
    if in_pw_xml:
        in_pw_root = parse_epg(in_pw_xml)
        in_pw_ch, in_pw_prog = extract_channels_and_programmes(
            in_pw_root, channel_ids=TARGET_CHANNEL_IDS
        )
        all_channels.update(in_pw_ch)
        all_programmes.extend(in_pw_prog)
        found = len(in_pw_ch)
        print(f"   → India (epg.pw): {found} matching channels | {len(in_pw_prog)} programmes")
        if found < len(TARGET_CHANNEL_IDS):
            missing = TARGET_CHANNEL_IDS - set(in_pw_ch.keys())
            print(f"      Missing IDs: {', '.join(sorted(missing))}")
        else:
            print("      All requested IDs found!")
        print("")
    else:
        print("   → epg.pw India EPG fetch failed - skipping\n")

    # 3. Australia (filtered)
    print(f"3. Fetching AU EPG (filtered: {', '.join(AU_KEYWORDS)})...")
    au_ch, au_prog = fetch_with_backups("AU", AU_EPG_URLS, keywords=AU_KEYWORDS)
    all_channels.update(au_ch)
    all_programmes.extend(au_prog)
    print(f"   → AU: {len(au_ch)} channels | {len(au_prog)} programmes\n")

    # 4. US (Willow Sports)
    print(f"4. Fetching US EPG (filtered: {', '.join(US_KEYWORDS)})...")
    us_ch, us_prog = fetch_with_backups("US", US_EPG_URLS, keywords=US_KEYWORDS)
    all_channels.update(us_ch)
    all_programmes.extend(us_prog)
    print(f"   → US: {len(us_ch)} channels | {len(us_prog)} programmes\n")

    # Final output
    if not all_programmes:
        raise Exception("No programmes collected from any source!")

    tv = ET.Element('tv', {
        'generator-info-name': 'Merged EPG (UK + IN Jio + Selective Hindi/Movies/Entertainment from epg.pw + AU + US Willow)',
        'generator-info-url': 'GitHub Actions'
    })

    for ch in all_channels.values():
        tv.append(ch)
    for prog in all_programmes:
        tv.append(prog)

    xml_bytes = BytesIO()
    ET.ElementTree(tv).write(xml_bytes, encoding='utf-8', xml_declaration=True)
    compressed = gzip.compress(xml_bytes.getvalue())

    with open('epg.xml.gz', 'wb') as f:
        f.write(compressed)

    print(f"\n🎉 SUCCESS! epg.xml.gz generated")
    print(f"   Total Channels   : {len(all_channels)}")
    print(f"   Total Programmes : {len(all_programmes)}")
    print(f"   File size        : {len(compressed)/1024:.1f} KB")

except Exception as e:
    print(f"\n💥 FAILED: {e}")
    traceback.print_exc()
    exit(1)
