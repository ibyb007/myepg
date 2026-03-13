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

# ========================================
# CONFIG
# ========================================

UK_EPG_URL    = 'https://epg.pw/xmltv/epg_GB.xml.gz'
IN_EPG_URL    = 'https://avkb.short.gy/jioepg.xml.gz'
IN_EPG_PW_URL = 'https://epg.pw/xmltv/epg_IN.xml.gz'           # ← Selective source

# GitHub Actions secret
CUSTOM_EPG_URL = os.getenv('CUSTOM_EPG_URL')

UK_KEYWORDS    = ['sky', 'tnt sports', 'premier sports', 'bt sport', 'eurosport', 'itv', 'bbc']
CUSTOM_KEYWORDS = ['fox', 'AU: Kayo 4K', 'AU: BEIN', 'AU: ESPN', 'astro']

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

print("=== Starting EPG Merge (UK + IN(Jio) + IN(epg.pw selective) + Custom Filtered) ===\n")

all_channels = {}
all_programmes = []

try:
    # 1. UK Sports
    print("1. Fetching UK EPG...")
    uk_xml = fetch_epg(UK_EPG_URL)
    if uk_xml:
        uk_root = parse_epg(uk_xml)
        uk_ch, uk_prog = extract_channels_and_programmes(uk_root, keywords=UK_KEYWORDS)
        all_channels.update(uk_ch)
        all_programmes.extend(uk_prog)
        print(f"   → UK: {len(uk_ch)} channels | {len(uk_prog)} programmes\n")

    # 2. India (Jio - Hindi/English only)
    print("2. Fetching India EPG (Jio)...")
    in_xml = fetch_epg(IN_EPG_URL)
    if in_xml:
        in_root = parse_epg(in_xml)
        in_ch, in_prog = extract_channels_and_programmes(in_root)  # All first
        in_ch = filter_out_regional(in_ch)
        in_prog = [p for p in in_prog if p.attrib['channel'] in in_ch]
        all_channels.update(in_ch)
        all_programmes.extend(in_prog)
        print(f"   → India (Jio filtered): {len(in_ch)} channels | {len(in_prog)} programmes\n")

    # 2.5 India epg.pw - only the requested specific channels
    print(f"2.5 Fetching India EPG (epg.pw) - {len(TARGET_CHANNEL_IDS)} targeted channels...")
    in_pw_xml = fetch_epg(IN_EPG_PW_URL)
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

    # 3. Custom 3rd-party source (filtered)
    if CUSTOM_EPG_URL:
        print(f"3. Fetching Custom EPG (filtered: {', '.join(CUSTOM_KEYWORDS)})...")
        custom_xml = fetch_epg(CUSTOM_EPG_URL)
        if custom_xml:
            custom_root = parse_epg(custom_xml)
            cust_ch, cust_prog = extract_channels_and_programmes(custom_root, keywords=CUSTOM_KEYWORDS)
            all_channels.update(cust_ch)
            all_programmes.extend(cust_prog)
            print(f"   → Custom: {len(cust_ch)} channels | {len(cust_prog)} programmes\n")
        else:
            print("   → Custom EPG fetch failed\n")
    else:
        print("⚠️ CUSTOM_EPG_URL secret not set! Skipping custom source.\n")

    # Final output
    if not all_programmes:
        raise Exception("No programmes collected from any source!")

    tv = ET.Element('tv', {
        'generator-info-name': 'Merged EPG (UK + IN Jio + Selective Hindi/Movies/Entertainment from epg.pw + Custom)',
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
