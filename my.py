import requests
import gzip
from io import BytesIO
import xml.etree.ElementTree as ET
from datetime import datetime

def fetch_epg(url):
    """Fetch and decompress EPG XML from gzipped URL, or return plain XML if not gzipped."""
    response = requests.get(url)
    response.raise_for_status()
    content = response.content
    try:
        compressed = BytesIO(content)
        decompressed = gzip.GzipFile(fileobj=compressed)
        return decompressed.read().decode('utf-8')
    except:
        return content.decode('utf-8')

def parse_epg(xml_content):
    """Parse EPG XML content."""
    return ET.fromstring(xml_content)

def extract_channels_and_programmes(root, channel_keywords):
    """Extract channels and programmes matching keywords in display-name or all if keywords is None."""
    channels = {}
    programmes = []

    # Extract channels
    if channel_keywords is None:
        for channel in root.findall('.//channel'):
            channels[channel.attrib['id']] = channel
    else:
        for channel in root.findall('.//channel'):
            display_names = [dn.text for dn in channel.findall('display-name') if dn.text]
            for keyword in channel_keywords:
                if any(keyword.lower() in name.lower() for name in display_names):
                    channels[channel.attrib['id']] = channel
                    break

    # Extract matching programmes
    for programme in root.findall('.//programme'):
        if programme.attrib['channel'] in channels:
            programmes.append(programme)

    return channels, programmes

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
AU_EPG_URL = 'https://epg.pw/xmltv/epg_AU.xml'
IN_EPG_URL = 'https://avkb.short.gy/epg.xml.gz'

# Static keywords for UK and AU
UK_KEYWORDS = ['sky sports', 'tnt sports']
AU_KEYWORDS = ['fox']
IN_KEYWORDS = None  # Include all from IN source

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

    # Combine
    all_channels = {**uk_channels, **au_channels, **in_channels}
    all_programmes = uk_programmes + au_programmes + in_programmes

    combined_root = create_combined_epg(all_channels, all_programmes)

    # Write to gzipped file
    tree = ET.ElementTree(combined_root)
    output = BytesIO()
    tree.write(output, encoding='utf-8', xml_declaration=True)
    output.seek(0)
    compressed_data = gzip.compress(output.read())
    with open('epg.xml.gz', 'wb') as f:
        f.write(compressed_data)

    print(f"EPG generated successfully as epg.xml.gz. Found {len(all_channels)} channels and {len(all_programmes)} programmes.")
    print("Channels:")
    for ch_id, ch in all_channels.items():
        display = ch.find('display-name')
        if display is not None:
            print(f"- {display.text} ({ch_id})")

except Exception as e:
    print(f"Error fetching or processing EPG: {e}")
