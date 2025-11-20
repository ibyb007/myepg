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

def extract_channels_and_programmes(root, keywords=None):
    channels = {}
    programmes = []

    # Filter channels by keywords if provided
    for channel in root.findall('.//channel'):
        ch_id = channel.attrib['id']
        display_names = [dn.text.lower() for dn in channel.findall('display-name') if dn.text]
        
        if keywords:
            if not any(any(kw in name for kw in keywords) for name in display_names):
                continue  # Skip non-matching channel
        channels[ch_id] = channel

    # Programmes (next 8 days)
    now = datetime.now()
    cutoff = now + timedelta(days=8)
    for prog in root.findall('.//programme'):
        if prog.attrib['channel'] in channels:
            start_str = prog.attrib.get('start', '')
            if len(start_str) >= 14:
                try:
                    start_dt = datetime.strptime(start_str[:14], '%Y%m%d%H%M%S')
                    if start_dt < now - timedelta(days=1):  # Skip too old
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

UK_EPG_URL = 'https://epg.pw/xmltv/epg_GB.xml.gz'
IN_EPG_URL = 'https://avkb.short.gy/epg.xml.gz'

# THIS COMES FROM GITHUB ACTIONS SECRET (Settings → Secrets and variables → Actions)
CUSTOM_EPG_URL = os.getenv('CUSTOM_EPG_URL')  # e.g. http://snaptv.lol:80/xmltv.php?username=xxx&password=xxx

UK_KEYWORDS = ['sky', 'tnt sports', 'premier sports', 'bt sport', 'eurosport', 'itv', 'bbc']
CUSTOM_KEYWORDS = ['fox', 'AU: Kayo 4K', 'AU: BEIN', 'AU: ESPN', 'astro']  # ← Your requested filters

print("=== Starting EPG Merge (UK + IN + Custom Filtered) ===\n")

all_channels = {}
all_programmes = []

try:
    # 1. UK Sports
    print("1. Fetching UK EPG...")
    uk_xml = fetch_epg(UK_EPG_URL)
    if uk_xml:
        uk_root = parse_epg(uk_xml)
        uk_ch, uk_prog = extract_channels_and_programmes(uk_root, UK_KEYWORDS)
        all_channels.update(uk_ch)
        all_programmes.extend(uk_prog)
        print(f"   → UK: {len(uk_ch)} channels | {len(uk_prog)} programmes\n")

    # 2. India (Hindi/English only)
    print("2. Fetching India EPG...")
    in_xml = fetch_epg(IN_EPG_URL)
    if in_xml:
        in_root = parse_epg(in_xml)
        in_ch, in_prog = extract_channels_and_programmes(in_root)  # All first
        in_ch = filter_out_regional(in_ch)
        in_prog = [p for p in in_prog if p.attrib['channel'] in in_ch]
        all_channels.update(in_ch)
        all_programmes.extend(in_prog)
        print(f"   → India (filtered): {len(in_ch)} channels | {len(in_prog)} programmes\n")

    # 3. Custom 3rd-party source (filtered by fox/kayo/astro)
    if CUSTOM_EPG_URL:
        print(f"3. Fetching Custom EPG (filtered: {', '.join(CUSTOM_KEYWORDS)})...")
        custom_xml = fetch_epg(CUSTOM_EPG_URL)
        if custom_xml:
            custom_root = parse_epg(custom_xml)
            cust_ch, cust_prog = extract_channels_and_programmes(custom_root, CUSTOM_KEYWORDS)
            all_channels.update(cust_ch)
            all_programmes.extend(cust_prog)
            print(f"   → Custom (fox/kayo/astro): {len(cust_ch)} channels | {len(cust_prog)} programmes\n")
        else:
            print("   → Custom EPG fetch failed\n")
    else:
        print("⚠️  CUSTOM_EPG_URL secret not set! Skipping custom source.\n")

    # Final output
    if not all_programmes:
        raise Exception("No programmes collected from any source!")

    tv = ET.Element('tv', {
        'generator-info-name': 'Merged Sports EPG (UK+IN+Custom)',
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
