import requests
import gzip
from io import BytesIO
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time  # For retries

def fetch_epg(url, max_retries=3):
    """Fetch and decompress EPG XML from gzipped URL, or return plain XML if not gzipped."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://epg.pw/'  # Extra header to mimic browser
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            content = response.content
            print(f"Fetched {url}: {len(content)} bytes")
            if len(content) < 100:  # Likely empty/error page
                print(f"Warning: Short response from {url} - possible block")
            try:
                compressed = BytesIO(content)
                decompressed = gzip.GzipFile(fileobj=compressed)
                xml_str = decompressed.read().decode('utf-8')
                print(f"Decompressed {url}: {len(xml_str)} chars")
                return xml_str
            except:
                return content.decode('utf-8')
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

def parse_epg(xml_content):
    """Parse EPG XML content."""
    if len(xml_content) < 100:
        raise ValueError("XML content too short - likely fetch error")
    return ET.fromstring(xml_content)

def extract_channels_and_programmes(root, channel_keywords):
    """Extract channels and programmes matching keywords in display-name or all if keywords is None."""
    channels = {}
    programmes = []

    # Extract channels
    matched_channels = []
    if channel_keywords is None:
        for channel in root.findall('.//channel'):
            channels[channel.attrib['id']] = channel
    else:
        for channel in root.findall('.//channel'):
            display_names = [dn.text for dn in channel.findall('display-name') if dn.text]
            for keyword in channel_keywords:
                if any(keyword.lower() in name.lower() for name in display_names):
                    channels[channel.attrib['id']] = channel
                    matched_channels.append((channel.attrib['id'], display_names[0] if display_names else ''))
                    break

    print(f"Extracted {len(channels)} channels (matches: {matched_channels})")

    # Extract matching programmes (next 7 days only for relevance)
    now = datetime.now()
    future_cutoff = now + timedelta(days=7)
    for programme in root.findall('.//programme'):
        if programme.attrib['channel'] in channels:
            start_str = programme.attrib['start']
            if len(start_str) >= 12:
                start_dt = datetime.strptime(start_str[:12], '%Y%m%d%H%M')
                if now <= start_dt <= future_cutoff:
                    programmes.append(programme)
            else:
                programmes.append(programme)  # Fallback if malformed

    print(f"Extracted {len(programmes)} relevant programmes")
    return channels, programmes

def filter_in_channels(channels_dict, exclude_languages):
    """Filter IN channels to exclude specified languages based on display-names."""
    filtered_channels = {}
    excluded_count = 0
    for ch_id, ch in channels_dict.items():
        display_names = [dn.text.lower() for dn in ch.findall('display-name') if dn.text]
        
        # Check for excluded languages (substring match in any display-name)
        exclude_lang = any(
            any(lang.lower() in name for lang in exclude_languages)
            for name in display_names
        )
        
        if not exclude_lang:
            filtered_channels[ch_id] = ch
        else:
            excluded_count += 1
    
    print(f"IN filter: Kept {len(filtered_channels)}/{len(channels_dict)} channels (excluded {excluded_count} for languages)")
    return filtered_channels

def create_combined_epg(channels_dict, all_programmes):
    """Create combined EPG XML."""
    tv = ET.Element('tv')
    tv.set('generator-info-name', 'Custom Sports EPG Fetcher')
    tv.set('generator-info-url', 'https://example.com')

    # Add channels
    for channel in channels_dict.values():
        tv.append(channel)

    # Add programmes
    for programme in all_programmes:
        tv.append(programme)

    return tv

# URLs for EPG sources
UK_EPG_URL = 'https://epg.pw/xmltv/epg_GB.xml.gz'
AU_EPG_URL = 'https://epg.pw/xmltv/epg_AU.xml.gz'
IN_EPG_URL = 'https://avkb.short.gy/epg.xml.gz'

# Broader keywords for UK and AU
UK_KEYWORDS = ['sky', 'tnt sports', 'skysp']  # Fallback to catch variations
AU_KEYWORDS = ['fox', 'channel 7']  # Added common AU sports
IN_KEYWORDS = None  # Include all from IN source initially

# Languages to exclude in channel titles (case-insensitive)
EXCLUDE_LANGUAGES = ['tamil', 'telugu', 'oriya', 'gujarati', 'kannada', 'malayalam', 'bhojpuri', 'punjabi', 'marathi']

try:
    # Fetch and parse UK EPG
    uk_xml = fetch_epg(UK_EPG_URL)
    uk_root = parse_epg(uk_xml)
    uk_channels, uk_programmes = extract_channels_and_programmes(uk_root, UK_KEYWORDS)

    # Fetch and parse AU EPG
    au_xml = fetch_epg(AU_EPG_URL)
    au_root = parse_epg(au_xml)
    au_channels, au_programmes = extract_channels_and_programmes(au_root, AU_KEYWORDS)

    # Fetch and parse IN EPG
    in_xml = fetch_epg(IN_EPG_URL)
    in_root = parse_epg(in_xml)
    in_channels, in_programmes = extract_channels_and_programmes(in_root, IN_KEYWORDS)

    # Filter IN channels to exclude specified languages only
    filtered_in_channels = filter_in_channels(in_channels, EXCLUDE_LANGUAGES)
    
    # Filter IN programmes to match filtered channels
    filtered_in_programmes = [p for p in in_programmes if p.attrib['channel'] in filtered_in_channels]

    # Combine
    all_channels = {**uk_channels, **au_channels, **filtered_in_channels}
    all_programmes = uk_programmes + au_programmes + filtered_in_programmes

    print(f"Combined: {len(all_channels)} channels, {len(all_programmes)} programmes")

    if all_programmes:
        sample_prog = all_programmes[0]
        print(f"Sample programme: {sample_prog.find('title').text if sample_prog.find('title') is not None else 'No title'} on channel {sample_prog.attrib['channel']}")

    combined_root = create_combined_epg(all_channels, all_programmes)

    # Write to gzipped file
    tree = ET.ElementTree(combined_root)
    output = BytesIO()
    tree.write(output, encoding='utf-8', xml_declaration=True)
    output.seek(0)
    xml_bytes = output.read()
    print(f"Generated XML: {len(xml_bytes)} bytes")
    compressed_data = gzip.compress(xml_bytes)
    print(f"Compressed: {len(compressed_data)} bytes")
    with open('epg.xml.gz', 'wb') as f:
        f.write(compressed_data)

    print(f"EPG generated successfully as epg.xml.gz. Found {len(all_channels)} channels and {len(all_programmes)} programmes.")
    print("Channels:")
    for ch_id, ch in list(all_channels.items())[:10]:  # First 10 to avoid spam
        display = ch.find('display-name')
        if display is not None:
            print(f"- {display.text} ({ch_id})")
    if len(all_channels) > 10:
        print(f"... and {len(all_channels) - 10} more")

except Exception as e:
    print(f"Error fetching or processing EPG: {e}")
    import traceback
    traceback.print_exc()
