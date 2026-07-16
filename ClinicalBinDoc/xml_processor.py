#!/usr/bin/env python
"""Converted from xml_processor.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
import xmltodict
import json
import re
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree
from sqlalchemy import create_engine, text, inspect

class Preprocessor:
    def __init__(self):
        pass

    def clean_xml_string(self, xml_string):
        try:
            if xml_string.startswith("b'"):
                xml_string = xml_string[2:-1]
            # Suppress deprecation warnings for unicode_escape decoding
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=DeprecationWarning)
                xml_string = xml_string.encode('utf-8').decode('unicode_escape')
            xml_string = xml_string.replace('((URL))', 'http://example.com')
            xml_string = re.sub(r'\(\((.*?)\)\)', lambda m: f'placeholder_{m.group(1)}', xml_string)
            xml_string = re.sub(r'<address>(.*?)</address>',
                                lambda m: f'<address>{m.group(1).replace("<", "&lt;").replace(">", "&gt;")}</address>',
                                xml_string)
            xml_string = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_string)
            xml_string = str(BeautifulSoup(xml_string, "xml"))
            xml_string = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', xml_string)
            return xml_string
        except Exception:
            return xml_string

    def validate_xml(self, xml_string):
        try:
            etree.fromstring(xml_string.encode('utf-8'))
            return True
        except etree.XMLSyntaxError:
            return False

    def clean_cdata(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str):
                    value = re.sub(r'<!\[CDATA\[.*?<FONT.*?>(.*?)</FONT>.*?\]\]>', r'\1', value)
                    value = re.sub(r'<[^>]+>', '', value)
                    value = ' '.join(value.split())
                    data[key] = value
                elif isinstance(value, (dict, list)):
                    self.clean_cdata(value)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self.clean_cdata(item)
        return data

    def xml_to_json(self, xml_string):
        try:
            cleaned_xml = self.clean_xml_string(xml_string)
            if not self.validate_xml(cleaned_xml):
                return None
            xml_dict = xmltodict.parse(cleaned_xml, disable_entities=True)
            cleaned_dict = self.clean_cdata(xml_dict)
            json_string = json.dumps(cleaned_dict, indent=2)
            return json_string
        except Exception:
            return None

    def process_xml(self, xml_data):
        json_output = self.xml_to_json(xml_data)
        return json_output if json_output else None

    def extract_body_content(self, json_data):
        """Extract body content from medical document JSON"""
        try:
            if isinstance(json_data, str):
                json_data = json.loads(json_data)
            
            # Navigate to body section
            if 'levelone' in json_data and 'body' in json_data['levelone']:
                return json_data['levelone']['body']
            return None
        except Exception:
            return None

    def parse_list_items(self, list_data):
        """Parse list items from section"""
        items = []
        if isinstance(list_data, dict) and 'item' in list_data:
            if isinstance(list_data['item'], list):
                for item in list_data['item']:
                    if isinstance(item, dict) and 'content' in item:
                        items.append(item['content'])
            else:
                if isinstance(list_data['item'], dict) and 'content' in list_data['item']:
                    items.append(list_data['item']['content'])
        return items

    def parse_table_data(self, table_data):
        """Parse table data and return structured format"""
        if not isinstance(table_data, dict) or 'tr' not in table_data:
            return {}

        table_dict = {}
        rows = table_data['tr']

        # Ensure rows is a list
        if not isinstance(rows, list):
            rows = [rows]

        # Skip the header row (if present)
        for i, row in enumerate(rows):
            if isinstance(row, dict):
                # Check for header row (contains 'th' elements)
                header = row.get('th', [])
                if header:
                    continue  # Skip header row

                # Process data rows (rows without 'th' elements)
                if 'td' in row:
                    cells = row['td']
                    if isinstance(cells, list) and len(cells) >= 2:
                        # If there are multiple columns, process as usual
                        key = cells[0].get('#text', '') if isinstance(cells[0], dict) else str(cells[0])

                        # Create sub-dictionary for this item
                        item_data = {}
                        for j, cell in enumerate(cells[1:], 1):
                            cell_value = cell.get('#text', '') if isinstance(cell, dict) else str(cell)
                            if cell_value and cell_value.strip() != '--':
                                # Use column headers or generic names
                                if j == 1:
                                    item_data['Status'] = cell_value
                                elif j == 2:
                                    item_data['Start/Stop'] = cell_value
                                elif j == 3:
                                    item_data['Quantity'] = cell_value
                                elif j == 4:
                                    item_data['Notes'] = cell_value

                        # Always add the key, even if all values are '--'
                        table_dict[key] = item_data if item_data else None
                    else:
                        # Handle single td case (like Social History, Family Medical History)
                        key = cells.get('#text', '') if isinstance(cells, dict) else str(cells)
                        table_dict[key] = {'Status': 'N/A'}  # Default 'Status' if needed

        return table_dict

    def parse_vitals(self, vitals_text):
        """Parse vitals text into structured format"""
        if not vitals_text:
            return {}
        
        # Clean up the text
        text = vitals_text.strip()
        
        # The format varies between samples, so let's handle different cases
        import re
        
        # Look for the date pattern to find where data starts
        date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
        date_match = re.search(date_pattern, text)
        
        if not date_match:
            return {"Raw_Vitals": text}
        
        # Extract the header part (before the date)
        header_part = text[:date_match.start()].strip()
        data_part = text[date_match.start():].strip()
        
        # Parse headers - handle different formats
        if 'Date Time BP Position Site L\\R Cuff Size HR RR TEMP' in header_part:
            # Standard format with spaces
            headers = ['Date', 'Time', 'BP', 'Position', 'Site', 'L\\R', 'Cuff', 'Size', 'HR', 'RR', 'TEMP( F)', 'WT', 'HT', 'BMI', 'kg/m2', 'BSA', 'm2', 'O2', 'Sat', 'HC']
        elif 'DateTimeBPPositionSiteL&#92;RCuff SizeHRRRTEMP' in header_part:
            # Compact format without spaces
            headers = ['DateTime', 'BP', 'Position', 'Site', 'L\\R', 'Cuff', 'Size', 'HR', 'RR', 'TEMP (F)', 'WT', 'HT', 'BMI', 'kg/m2', 'BSA', 'm2', 'O2', 'Sat', 'FR', 'L/min', 'FiO2', 'HC']
        else:
            # Try to extract headers dynamically
            header_part_clean = re.sub(r'[{}]', '', header_part)
            headers = [h.strip() for h in header_part_clean.split() if h.strip()]
        
        # Parse the data part
        # Handle the date/time combination first
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)'
        dt_match = re.search(date_time_pattern, data_part)
        
        if dt_match:
            date_val = dt_match.group(1)
            time_val = dt_match.group(2)
            
            # Get the remaining data after date/time
            remaining_data = data_part[dt_match.end():].strip()
            
            # Split remaining data into parts, but be careful with units
            # Handle cases where values have units attached
            parts = []
            current_part = ""
            in_quotes = False
            
            for char in remaining_data:
                if char == "'" or char == '"':
                    in_quotes = not in_quotes
                    current_part += char
                elif char == ' ' and not in_quotes:
                    if current_part.strip():
                        parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            # Create vitals dictionary
            vitals = {
                'Date': date_val,
                'Time': time_val
            }
            
            # Based on pattern analysis, map the values correctly
            # The pattern is: BP, Position, Site(TEMP), L\R, Cuff, Size(WT), [missing HR], [missing RR], HT, BMI, BSA
            if len(parts) >= 6:
                vitals['BP'] = parts[0]
                vitals['Position'] = parts[1]
                vitals['TEMP( F)'] = parts[2]  # Site is actually TEMP
                vitals['Site'] = None
                vitals['L\\R'] = parts[3]
                vitals['Cuff'] = parts[4]
                vitals['WT'] = parts[5]  # Size is actually WT
                vitals['Size'] = None
                vitals['HR'] = None  # Missing
                vitals['RR'] = None  # Missing
                
                # Map remaining values based on their characteristics
                remaining_parts = parts[6:]
                
                # Find height (contains ' or ")
                ht_value = None
                bmi_value = None
                bsa_value = None
                
                for part in remaining_parts:
                    if "'" in part or '"' in part:
                        ht_value = part
                    else:
                        # Check if it's a numeric value that could be BMI or BSA
                        try:
                            float_val = float(part)
                            if bmi_value is None:
                                bmi_value = part
                            elif bsa_value is None:
                                bsa_value = part
                        except ValueError:
                            pass
                
                vitals['HT'] = ht_value
                vitals['BMI'] = bmi_value
                vitals['BSA'] = bsa_value
                vitals['kg/m2'] = None
                vitals['m2'] = None
                vitals['O2'] = None
                vitals['Sat'] = None
                vitals['HC'] = None
            
            return vitals
        else:
            # Fallback: return as is
            return {"Raw_Vitals": text}

    def parse_text_data(self, text_data):
        """Parse text data from local_markup or content"""
        if isinstance(text_data, dict):
            if 'datahtml' in text_data:
                datahtml = text_data['datahtml']
                if isinstance(datahtml, dict):
                    # Handle sketchpads structure
                    if 'sketchpads' in datahtml:
                        sketchpads = datahtml['sketchpads']
                        if isinstance(sketchpads, dict) and 'sketchpad' in sketchpads:
                            sketchpad = sketchpads['sketchpad']
                            if isinstance(sketchpad, dict) and 'text' in sketchpad:
                                text = sketchpad['text']
                                if isinstance(text, dict) and '@data' in text:
                                    return text['@data']
                    
                    # Handle div structure
                    elif 'div' in datahtml:
                        div = datahtml['div']
                        if isinstance(div, dict) and '#text' in div:
                            return div['#text']
                    
                    # Handle other structures
                    elif '#text' in datahtml:
                        return datahtml['#text']
                
                # Handle CSS + text mixed content (like Vitals)
                elif isinstance(datahtml, str):
                    # Extract text content from CSS + text mixed content
                    # Remove CSS and extract the actual data
                    text_content = datahtml
                    # Remove CSS rules
                    text_content = re.sub(r'\.\w+\s*{[^}]*}', '', text_content)
                    # Remove HTML entities and clean up
                    text_content = re.sub(r'&nbsp;', ' ', text_content)
                    text_content = re.sub(r'\\"', '"', text_content)
                    text_content = re.sub(r'\\n', ' ', text_content)
                    text_content = re.sub(r'\\t', ' ', text_content)
                    # Clean up multiple spaces
                    text_content = re.sub(r'\s+', ' ', text_content).strip()
                    return text_content
                    
            elif 'content' in text_data:
                return text_data['content']
        elif isinstance(text_data, str):
            return text_data
        return None

    def format_hierarchical_text(self, data, indent_level=0):
        """Format hierarchical data as indented text without brackets"""
        if isinstance(data, dict):
            lines = []
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{'    ' * indent_level}{key}:")
                    lines.extend(self.format_hierarchical_text(value, indent_level + 1))
                else:
                    lines.append(f"{'    ' * indent_level}{key}: {value}")
            return lines
        elif isinstance(data, list):
            lines = []
            for item in data:
                if isinstance(item, dict):
                    lines.extend(self.format_hierarchical_text(item, indent_level))
                else:
                    lines.append(f"{'    ' * indent_level}- {item}")
            return lines
        else:
            return [f"{'    ' * indent_level}{data}"]

    def parse_hierarchical_list(self, list_data, indent_level=0):
        """Parse hierarchical list structures recursively"""
        result = {}
        
        if isinstance(list_data, dict) and 'item' in list_data:
            items = list_data['item']
            if not isinstance(items, list):
                items = [items]
            
            for item in items:
                if isinstance(item, dict):
                    caption = item.get('caption', '')
                    content = item.get('content', '')
                    
                    if isinstance(content, dict) and 'list' in content:
                        # Recursive call for nested lists
                        nested_result = self.parse_hierarchical_list(content['list'], indent_level + 1)
                        if nested_result:
                            result[caption] = nested_result
                    else:
                        result[caption] = content
        
        elif isinstance(list_data, list):
            # Handle case where list_data is directly a list
            for item in list_data:
                if isinstance(item, dict):
                    # Check if item has 'item' key (wrapped structure)
                    if 'item' in item:
                        actual_item = item['item']
                        if isinstance(actual_item, dict):
                            caption = actual_item.get('caption', '')
                            content = actual_item.get('content', '')
                            
                            if isinstance(content, dict) and 'list' in content:
                                # Recursive call for nested lists
                                nested_result = self.parse_hierarchical_list(content['list'], indent_level + 1)
                                if nested_result:
                                    result[caption] = nested_result
                            else:
                                result[caption] = content
                    else:
                        # Direct item structure
                        caption = item.get('caption', '')
                        content = item.get('content', '')
                        
                        if isinstance(content, dict) and 'list' in content:
                            # Recursive call for nested lists
                            nested_result = self.parse_hierarchical_list(content['list'], indent_level + 1)
                            if nested_result:
                                result[caption] = nested_result
                        else:
                            result[caption] = content
        
        return result

    def parse_physical_examination(self, section_data):
        """Parse Physical Examination with deep hierarchical structure"""
        result = {}
        
        if isinstance(section_data, dict) and 'section' in section_data:
            subsections = section_data['section']
            if not isinstance(subsections, list):
                subsections = [subsections]
            
            for subsection in subsections:
                if isinstance(subsection, dict):
                    caption = subsection.get('caption', '')
                    if isinstance(caption, dict) and '#text' in caption:
                        caption = caption['#text']
                    
                    if caption and 'list' in subsection:
                        list_data = subsection['list']
                        subsection_result = self.parse_hierarchical_list(list_data)
                        if subsection_result:
                            result[caption] = subsection_result
        
        return result

    def parse_review_of_systems(self, section_data):
        """Parse Review of Systems with Denies/Admits captions"""
        result = {}
        
        if isinstance(section_data, dict) and 'section' in section_data:
            subsections = section_data['section']
            if not isinstance(subsections, list):
                subsections = [subsections]
            
            for subsection in subsections:
                if isinstance(subsection, dict):
                    caption = subsection.get('caption', '')
                    if isinstance(caption, dict) and '#text' in caption:
                        caption = caption['#text']
                    
                    if caption and 'list' in subsection:
                        list_data = subsection['list']
                        if isinstance(list_data, dict) and 'item' in list_data:
                            items = list_data['item']
                            if not isinstance(items, list):
                                items = [items]
                            
                            subsection_result = {}
                            for item in items:
                                if isinstance(item, dict):
                                    item_caption = item.get('caption', '')
                                    item_content = item.get('content', '')
                                    
                                    if item_caption in ['Denies', 'Admits']:
                                        if isinstance(item_content, str):
                                            subsection_result[item_caption] = [item_content]
                                        elif isinstance(item_content, list):
                                            subsection_result[item_caption] = item_content
                            
                            if subsection_result:
                                result[caption] = subsection_result
        
        return result

    def parse_assessment(self, assessment_data):
        """Parse Assessment section with diagnosis codes"""
        result = []
        
        if isinstance(assessment_data, dict) and 'list' in assessment_data:
            list_data = assessment_data['list']
            if isinstance(list_data, dict) and 'item' in list_data:
                items = list_data['item']
                if not isinstance(items, list):
                    items = [items]
                
                for item in items:
                    if isinstance(item, dict) and 'content' in item:
                        content = item['content']
                        if isinstance(content, list):
                            for content_item in content:
                                if isinstance(content_item, dict):
                                    # Handle new structure with coded_entry and #text
                                    if 'coded_entry' in content_item and '#text' in content_item:
                                        coded_info = content_item.get('coded_entry', {})
                                        if isinstance(coded_info, dict) and 'coded_entry.value' in coded_info:
                                            value_info = coded_info['coded_entry.value']
                                            if isinstance(value_info, dict) and '@V' in value_info:
                                                code = value_info['@V']
                                                description = content_item.get('#text', '')
                                                diagnosis = {
                                                    'description': description,
                                                    'code': code
                                                }
                                                result.append(diagnosis)
                                    # Handle old structure with dxdesc and dxcode
                                    elif 'dxdesc' in content_item and 'dxcode' in content_item:
                                        diagnosis = {
                                            'description': content_item['dxdesc'],
                                            'code': content_item['dxcode']
                                        }
                                        result.append(diagnosis)
        
        return result

    def parse_plan_section(self, plan_data):
        """Parse Plan section with Orders, Medications, etc."""
        result = {}
        
        # Handle both list and single section
        if isinstance(plan_data, list):
            subsections = plan_data
        else:
            # Single section case
            subsections = [plan_data]
        
        for subsection in subsections:
            if isinstance(subsection, dict):
                caption = subsection.get('caption', '')
                if caption and 'list' in subsection:
                    list_data = subsection['list']
                    if isinstance(list_data, dict) and 'item' in list_data:
                        items = list_data['item']
                        if not isinstance(items, list):
                            items = [items]
                        
                        parsed_items = []
                        for item in items:
                            if isinstance(item, dict) and 'content' in item:
                                content = item['content']
                                if isinstance(content, dict):
                                    if '#text' in content:
                                        parsed_items.append(content['#text'])
                                    elif 'coded_entry' in content:
                                        # Extract coded entry information
                                        coded_info = content.get('coded_entry', {})
                                        if isinstance(coded_info, dict) and 'coded_entry.value' in coded_info:
                                            value_info = coded_info['coded_entry.value']
                                            if isinstance(value_info, dict) and '@V' in value_info:
                                                code = value_info['@V']
                                                text = content.get('#text', '')
                                                parsed_items.append(f"{text} (Code: {code})")
                                        else:
                                            parsed_items.append(content.get('#text', str(content)))
                                else:
                                    parsed_items.append(str(content))
                        
                        if parsed_items:
                            result[caption] = parsed_items
        
        return result

    def parse_section_to_json(self, section):
        """Parse individual section and return structured JSON"""
        if not isinstance(section, dict):
            return None
        
        # Get caption
        caption = section.get('caption', '')
        if isinstance(caption, dict):
            if '#text' in caption:
                caption = caption['#text']
            elif 'caption_cd' in caption:
                caption = caption.get('#text', '')
        
        # Handle correspondence documents (no caption, just paragraphs)
        if caption is None and 'paragraph' in section:
            paragraphs = section['paragraph']
            result = {}
            
            if isinstance(paragraphs, list):
                for i, para in enumerate(paragraphs):
                    if isinstance(para, dict):
                        title = para.get('@title', '')
                        content = para.get('content', '')
                        
                        if title and title.strip():
                            # Parse content
                            if isinstance(content, str):
                                result[title] = content
                            elif isinstance(content, list):
                                # Extract text from content list
                                text_parts = []
                                for item in content:
                                    if isinstance(item, str) and item.strip():
                                        text_parts.append(item)
                                    elif isinstance(item, dict) and '#text' in item:
                                        text_parts.append(item['#text'])
                                if text_parts:
                                    result[title] = '\n'.join(text_parts)
                            elif content is None:
                                result[title] = ""
                        else:
                            # Handle content without title - create individual entries
                            if isinstance(content, str) and content.strip():
                                result[f"Content_{i}"] = content
                            elif isinstance(content, list):
                                text_parts = []
                                for item in content:
                                    if isinstance(item, str) and item.strip():
                                        text_parts.append(item)
                                    elif isinstance(item, dict) and '#text' in item:
                                        text_parts.append(item['#text'])
                                if text_parts:
                                    # Try to extract a meaningful label from the content
                                    content_text = '\n'.join(text_parts)
                                    if ':' in content_text:
                                        # Extract the part before the first colon as a label
                                        label = content_text.split(':')[0].strip()
                                        if label and len(label) < 50:  # Reasonable label length
                                            result[label] = content_text
                                        else:
                                            result[f"Content_{i}"] = content_text
                                    else:
                                        result[f"Content_{i}"] = content_text
            else:
                if isinstance(paragraphs, dict) and 'content' in paragraphs:
                    content_text = paragraphs['content']
                    if isinstance(content_text, str):
                        result["Content"] = content_text
                    elif isinstance(content_text, list):
                        text_parts = []
                        for text_item in content_text:
                            if isinstance(text_item, str) and text_item.strip():
                                text_parts.append(text_item)
                        if text_parts:
                            result["Content"] = '\n'.join(text_parts)
            
            return result if result else None
        
        # Handle regular sections with captions
        if not caption:
            return None
        
        # Parse based on section type
        if 'list' in section:
            # List type (like Chief Complaint, Assessment)
            if caption == "Assessment":
                return self.parse_assessment(section)
            else:
                items = self.parse_list_items(section['list'])
                return items
        
        elif 'local_markup' in section:
            # Text data type (like History of Present Illness, Vitals)
            text = self.parse_text_data(section['local_markup'])
            if caption == "Vitals":
                return self.parse_vitals(text)
            else:
                return text
        
        elif 'table' in section:
            # Table type (like Past Medical History, Social History)
            return self.parse_table_data(section['table'])
        
        elif 'section' in section:
            # Hierarchical type (like Review of Systems, Physical Examination, Plan)
            if caption == "Review of Systems":
                return self.parse_review_of_systems(section)
            elif caption == "Plan":
                return self.parse_plan_section(section['section'])
            elif caption == "Physical Examination":
                return self.parse_physical_examination(section)
            else:
                return self.parse_hierarchical_list(section)
        
        elif 'content' in section:
            # Simple content type
            if isinstance(section['content'], str):
                return section['content']
            elif isinstance(section['content'], list):
                return [item for item in section['content'] if isinstance(item, str)]
        
        elif 'paragraph' in section:
            # Paragraph type
            paragraphs = section['paragraph']
            content = []
            if isinstance(paragraphs, list):
                for para in paragraphs:
                    if isinstance(para, dict) and 'content' in para:
                        content_text = para['content']
                        if isinstance(content_text, str):
                            content.append(content_text)
                        elif isinstance(content_text, list):
                            for text_item in content_text:
                                if isinstance(text_item, str):
                                    content.append(text_item)
            else:
                if isinstance(paragraphs, dict) and 'content' in paragraphs:
                    content_text = paragraphs['content']
                    if isinstance(content_text, str):
                        content.append(content_text)
            
            return '\n'.join(content) if content else None
        
        return None

    def parse_medical_document_to_json(self, json_data):
        """Parse medical document and extract structured JSON"""
        try:
            body_content = self.extract_body_content(json_data)
            if not body_content:
                return None
            
            # Handle documents with local_markup directly in body (like task/note documents)
            if 'local_markup' in body_content and 'section' not in body_content:
                text = self.parse_text_data(body_content['local_markup'])
                if text:
                    return {"Content": text}
                return None
            
            result = {}
            if 'section' in body_content:
                section_list = body_content['section']
                if isinstance(section_list, list):
                    for section in section_list:
                        parsed_section = self.parse_section_to_json(section)
                        if parsed_section is not None:
                            caption = section.get('caption', '')
                            if isinstance(caption, dict):
                                if '#text' in caption:
                                    caption = caption['#text']
                                elif 'caption_cd' in caption:
                                    caption = caption.get('#text', '')
                            
                            if caption:
                                result[caption] = parsed_section
                            else:
                                # Handle sections with caption: null (paragraph structure with @title)
                                if isinstance(parsed_section, dict):
                                    # Merge the parsed section into result
                                    result.update(parsed_section)
                                else:
                                    result["Content"] = parsed_section
                else:
                    parsed_section = self.parse_section_to_json(section_list)
                    if parsed_section is not None:
                        caption = section_list.get('caption', '')
                        if isinstance(caption, dict):
                            if '#text' in caption:
                                caption = caption['#text']
                            elif 'caption_cd' in caption:
                                caption = caption.get('#text', '')
                        
                        if caption:
                            result[caption] = parsed_section
                        else:
                            # Handle sections with caption: null
                            if isinstance(parsed_section, dict):
                                result.update(parsed_section)
                            else:
                                result["Content"] = parsed_section
            
            return result
        
        except Exception as e:
            print(f"Error parsing medical document: {str(e)}")
            return None

    def parse_medical_document_to_text(self, json_data):
        """Parse medical document JSON to continuous text format"""
        body_content = self.extract_body_content(json_data)
        if not body_content:
            return ""
        
        content_lines = []
        
        # Handle case where body has local_markup directly (task/note documents)
        if 'local_markup' in body_content:
            text = self.parse_text_data(body_content['local_markup'])
            if text:
                content_lines.append(text)
        elif 'section' in body_content:
            sections = body_content['section']
            if not isinstance(sections, list):
                sections = [sections]
            
            for section in sections:
                section_text = self.parse_section_to_text(section)
                if section_text:
                    if isinstance(section_text, list):
                        # Filter out empty strings from the list
                        filtered_text = [text for text in section_text if text and text.strip()]
                        if filtered_text:
                            content_lines.extend(filtered_text)
                    else:
                        # Only add non-empty strings
                        if section_text and section_text.strip():
                            content_lines.append(section_text)
        
        return '\n'.join(content_lines)

    def parse_section_to_text(self, section):
        """Parse section to formatted text"""
        if isinstance(section, dict):
            caption = section.get('caption', '')
            if isinstance(caption, dict) and '#text' in caption:
                caption = caption['#text']
            
            content = []
            
            if caption == "Chief Complaint":
                items = self.parse_list_items(section)
                content.append(f"{caption}:")
                for item in items:
                    content.append(f"    - {item}")
                return content
            
            elif caption == "History Of Present Illness":
                if 'local_markup' in section:
                    text = self.parse_text_data(section['local_markup'])
                elif 'paragraph' in section:
                    # Handle paragraph structure
                    paragraphs = section['paragraph']
                    if isinstance(paragraphs, list):
                        text_parts = []
                        for para in paragraphs:
                            if isinstance(para, dict) and 'content' in para:
                                content = para['content']
                                if isinstance(content, str):
                                    text_parts.append(content)
                                elif isinstance(content, list):
                                    # Filter out null values and join
                                    valid_parts = [part for part in content if part is not None]
                                    text_parts.extend(valid_parts)
                        text = '\n'.join(text_parts)
                    else:
                        text = str(paragraphs)
                else:
                    text = ""
                
                content.append(f"{caption}:")
                content.append(f"    {text}")
                return content
            
            elif caption in ["Past Medical History", "Social History"]:
                table_data = self.parse_table_data(section['table'])
                content.append(f"{caption}:")
                for key, value in table_data.items():
                    if isinstance(value, dict):
                        sub_items = []
                        for sub_key, sub_value in value.items():
                            sub_items.append(f"{sub_key}: {sub_value}")
                        content.append(f"    {key}: {', '.join(sub_items)}")
                    elif value is None:
                        content.append(f"    {key}: --")
                    else:
                        content.append(f"    {key}: {value}")
                return content
            
            elif caption == "Review of Systems":
                ros_data = self.parse_review_of_systems(section)
                content.append(f"{caption}:")
                for system, data in ros_data.items():
                    content.append(f"    {system}:")
                    for category, items in data.items():
                        content.append(f"        {category}:")
                        for item in items:
                            content.append(f"            - {item}")
                return content
            
            elif caption == "Vitals":
                vitals_text = self.parse_text_data(section['local_markup'])
                vitals_data = self.parse_vitals(vitals_text)
                content.append(f"{caption}:")
                for key, value in vitals_data.items():
                    content.append(f"    {key}: {value}")
                return content
            
            elif caption == "Physical Examination":
                physical_data = self.parse_physical_examination(section)
                content.append(f"{caption}:")
                for subsection, data in physical_data.items():
                    content.append(f"    {subsection}:")
                    content.extend(self.format_hierarchical_text(data, 2))
                return content
            
            elif caption == "Assessment":
                assessment_data = self.parse_assessment(section)
                content.append(f"{caption}:")
                for diagnosis in assessment_data:
                    content.append(f"    {diagnosis['description']} (Code: {diagnosis['code']})")
                return content
            
            elif caption == "Plan":
                if 'section' in section:
                    plan_data = self.parse_plan_section(section['section'])
                    content.append(f"{caption}:")
                    for section_name, items in plan_data.items():
                        content.append(f"    {section_name}:")
                        for item in items:
                            content.append(f"        - {item}")
                    return content
                else:
                    return []
            
            elif caption is None and 'paragraph' in section:
                # Handle paragraph structure with @title
                paragraphs = section['paragraph']
                if not isinstance(paragraphs, list):
                    paragraphs = [paragraphs]
                
                content = []
                for para in paragraphs:
                    if isinstance(para, dict):
                        title = para.get('@title', '')
                        para_content = para.get('content', '')
                        
                        if title and title.strip():
                            content.append(f"{title}:")
                            
                            # Parse content
                            if isinstance(para_content, str):
                                content.append(f"    {para_content}")
                            elif isinstance(para_content, list):
                                text_parts = []
                                for item in para_content:
                                    if isinstance(item, str) and item.strip():
                                        text_parts.append(item)
                                    elif isinstance(item, dict) and '#text' in item:
                                        text_parts.append(item['#text'])
                                if text_parts:
                                    content.append(f"    {' '.join(text_parts)}")
                            elif para_content is None:
                                content.append("    --")
                        else:
                            # Handle content without title - add as individual lines
                            if isinstance(para_content, str) and para_content.strip():
                                content.append(para_content)
                            elif isinstance(para_content, list):
                                text_parts = []
                                for item in para_content:
                                    if isinstance(item, str) and item.strip():
                                        text_parts.append(item)
                                    elif isinstance(item, dict) and '#text' in item:
                                        text_parts.append(item['#text'])
                                if text_parts:
                                    content.append(' '.join(text_parts))
                
                return content
            
            elif 'local_markup' in section:
                # Text data type (like History of Present Illness, Vitals)
                text = self.parse_text_data(section['local_markup'])
                if caption:
                    content.append(f"{caption}:")
                    content.append(f"    {text}")
                else:
                    content.append(text)
                return content
            
            else:
                # Default hierarchical list parsing
                hierarchical_data = self.parse_hierarchical_list(section)
                if caption:
                    content.append(f"{caption}:")
                for key, value in hierarchical_data.items():
                    if isinstance(value, dict):
                        content.append(f"    {key}:")
                        content.extend(self.format_hierarchical_text(value, 2))
                    else:
                        content.append(f"    {key}: {value}")
                return content
        
        return []

    def convert_json_to_text(self, input_data):
        """Convert medical document JSON to structured text"""
        try:
            parsed_text = self.parse_medical_document_to_text(input_data)
            return parsed_text
        except Exception:
            return None

    def convert_json_to_json(self, input_data):
        """Convert medical document JSON to hierarchical JSON"""
        try:
            parsed_json = self.parse_medical_document_to_json(input_data)
            return parsed_json
        except Exception:
            return None



    def process_single_xml(self, xml_content):
        try:

            json_data = self.process_xml(xml_content)
            if json_data:
                categorized_text = self.convert_json_to_text(json_data)
                hierarchical_json = self.convert_json_to_json(json_data)
                return {
                    "json_data": json_data,
                    "categorized_text": categorized_text,
                    "hierarchical_json": hierarchical_json
                }
            else:
                return {
                    "json_data": None,
                    "categorized_text": None,
                    "hierarchical_json": None
                }
        except Exception:
            return {
                "json_data": None,
                "categorized_text": None,
                "hierarchical_json": None
            }

    def process_csv_data(self, csv_file_path, output_file_path=None):
        """
        Process CSV file containing XML data in 'DocContent' column
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            print(f"Loaded CSV with {len(df)} rows")
            
            results = []
            preprocessor = Preprocessor()
            
            for idx, row in df.iterrows():
                try:
                    doc_id = row['DocumentID']
                    xml_content = row['DocContent']
                    
                    print(f"Processing document {idx+1}/{len(df)}: {doc_id}")
                    
                    # Process XML to JSON
                    json_data = preprocessor.process_xml(xml_content)
                    
                    if json_data:
                        # Convert to categorized text and JSON
                        categorized_text = preprocessor.convert_json_to_text(json_data)
                        hierarchical_json = preprocessor.convert_json_to_json(json_data)
                        
                        results.append({
                            'DocumentID': doc_id,
                            'SequenceNumber': row['SequenceNumber'],
                            'BinTypeID': row['BinTypeID'],
                            'JSON_Data': json_data,
                            'Categorized_Text': categorized_text,
                            'Hierarchical_JSON': json.dumps(hierarchical_json, indent=2) if hierarchical_json else None
                        })
                    else:
                        print(f"Failed to process XML for document {doc_id}")
                        results.append({
                            'DocumentID': doc_id,
                            'SequenceNumber': row['SequenceNumber'],
                            'BinTypeID': row['BinTypeID'],
                            'JSON_Data': None,
                            'Categorized_Text': None,
                            'Hierarchical_JSON': None
                        })
                        
                except Exception as e:
                    print(f"Error processing row {idx}: {str(e)}")
                    results.append({
                        'DocumentID': row.get('DocumentID', f'row_{idx}'),
                        'SequenceNumber': row.get('SequenceNumber', ''),
                        'BinTypeID': row.get('BinTypeID', ''),
                        'JSON_Data': None,
                        'Categorized_Text': None,
                        'Hierarchical_JSON': None
                    })
            
            # Create results DataFrame
            results_df = pd.DataFrame(results)
            
            # Save results
            if output_file_path:
                results_df.to_csv(output_file_path, index=False)
                print(f"Results saved to {output_file_path}")
            
            return results_df
            
        except Exception as e:
            print(f"Error processing CSV: {str(e)}")
            return None

    def safe_parse(self, x):
        import json as json_lib
        import ast
        """Return list of dicts with 'key' and 'val' for Polars explode."""
        if x is None:
            return []
        if isinstance(x, dict):
            result = []
            for k, v in x.items():
                # Convert value to string, handling nested structures
                if isinstance(v, (dict, list)):
                    try:
                        val_str = json_lib.dumps(v)
                    except Exception:
                        val_str = str(v)
                else:
                    val_str = str(v) if v is not None else ""
                result.append({"key": str(k), "val": val_str})
            return result
        if isinstance(x, str):
            try:
                d = json_lib.loads(x)
                if isinstance(d, dict):
                    result = []
                    for k, v in d.items():
                        if isinstance(v, (dict, list)):
                            try:
                                val_str = json_lib.dumps(v)
                            except Exception:
                                val_str = str(v)
                        else:
                            val_str = str(v) if v is not None else ""
                        result.append({"key": str(k), "val": val_str})
                    return result
            except Exception:
                try:
                    d = ast.literal_eval(x)
                    if isinstance(d, dict):
                        result = []
                        for k, v in d.items():
                            if isinstance(v, (dict, list)):
                                try:
                                    val_str = json_lib.dumps(v)
                                except Exception:
                                    val_str = str(v)
                            else:
                                val_str = str(v) if v is not None else ""
                            result.append({"key": str(k), "val": val_str})
                        return result
                except Exception:
                    return []
        return []

    def explode_json_categories_all_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Explodes JSON/dict columns to extract universal categories for each column in columns."""
        import polars as pl
        import json as json_lib

        # Normalize columns to ensure consistent types before Polars conversion
        def normalize_value(x):
            """Normalize values to be either None or a dict (not a list)"""
            if x is None:
                return None
            # If it's already a dict, return it
            if isinstance(x, dict):
                return x
            # If it's a list, extract the first dict element if present
            if isinstance(x, list):
                if len(x) > 0 and isinstance(x[0], dict):
                    return x[0]  # Return the dict from the list
                elif len(x) > 0:
                    # If list contains non-dict, convert to dict with index keys
                    return {str(i): str(v) for i, v in enumerate(x)}
                else:
                    return None  # Empty list becomes None
            # For other types, try to convert to string representation
            return str(x) if x is not None else None

        # Normalize the columns we want to explode
        # Convert dicts to JSON strings for Polars conversion (avoids type mixing issues)
        for col in columns:
            if col in df.columns:
                # Normalize to dict first
                df[col] = df[col].apply(normalize_value)
                # Convert dict to JSON string for Polars conversion (avoids type mixing issues)
                df[col] = df[col].apply(
                    lambda x: json_lib.dumps(x) if isinstance(x, dict) else None
                )
        
        # Also normalize all other object columns to prevent Polars conversion issues
        # Convert any lists/dicts in other columns to strings
        for col in df.columns:
            if col not in columns and df[col].dtype == 'object':
                def normalize_other_cols(x):
                    if x is None:
                        return None
                    if isinstance(x, (dict, list)):
                        try:
                            return json_lib.dumps(x)
                        except Exception:
                            return str(x)
                    return x
                df[col] = df[col].apply(normalize_other_cols)
        
        # Convert to Polars - all dicts are now JSON strings, so no type mixing
        # safe_parse already handles JSON strings, so we can keep them as strings
        pl_df = pl.from_pandas(df)
        
        for col in columns:
            if col not in pl_df.columns:
                continue
                
            # Map JSON/dict → list of structs for this column
            pl_df = pl_df.with_columns(
                pl.col(col).map_elements(
                    self.safe_parse,
                    return_dtype=pl.List(pl.Struct([
                        pl.Field("key", pl.Utf8),
                        pl.Field("val", pl.Utf8)
                    ]))
                ).alias(f"{col}_pairs")
            )

            # Explode this column
            pl_df = pl_df.explode(f"{col}_pairs")

            # Separate struct fields into columns
            pl_df = pl_df.with_columns([
                pl.col(f"{col}_pairs").struct.field("key").alias(f"{col}_category"),
                pl.col(f"{col}_pairs").struct.field("val").alias(col)
            ]).drop(f"{col}_pairs")

        return pl_df.to_pandas()

# %% [code cell 2]
def process_tables_mysql(engine, table_name):
    inspector = inspect(engine)
    with engine.begin() as conn:
        print(f"[MySQL] Processing table: {table_name}")

        # Check if column exists
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if 'nd_auto_increment_id' in columns:
            print(f"  Column 'nd_auto_increment_id' already exists. Skipping.")
            return False
            # try:
            #     conn.execute(text(f"ALTER TABLE `{table_name}` DROP COLUMN `nd_auto_increment_id`"))
            #     print(f"  Dropped existing column in {table_name}")
            # except Exception as e:
            #     print(f"  Error dropping column in {table_name}: {e}")
            #     continue

        # Add new column
        try:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `nd_auto_increment_id` BIGINT UNIQUE"))
            print(f"  Added column in {table_name}")
        except Exception as e:
            print(f"  Error adding column in {table_name}: {e}")
            return False

        # Populate sequential values
        try:
            conn.execute(text("SET @row_num = 0"))
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(
                f"UPDATE `{table_name}` SET `nd_auto_increment_id` = (@row_num := @row_num + 1)"
            ))
            print(f"  Populated sequential IDs in {table_name}")
            return True
        except Exception as e:
            print(f"  Error populating column in {table_name}: {e}")
            return False

# %% [code cell 3]
def stream_data(engine, table_name, batch_size=10000):
        """
        Yields rows from the CDC table in chunks using ID-based windowing.
        This prevents memory exhaustion and long-running transaction timeouts.
        """
        last_id = 0
        
        # 1. Get the total count once for progress tracking (optional)
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        total_rows = engine.connect().execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"Starting stream for {total_rows} rows...")

        while True:
            # 2. Fetch the next batch based on the last ID processed
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            query = text(f"""
                SELECT * FROM {table_name} 
                WHERE nd_auto_increment_id > :last_id 
                ORDER BY nd_auto_increment_id ASC 
                LIMIT :limit
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {"last_id": last_id, "limit": batch_size}).fetchall()
            
            # If no more rows are returned, we are done
            if not result:
                break
                
            for row in result:
                yield row
                last_id = row.nd_auto_increment_id  # Update the pointer to the last processed ID
                
            # Print progress every batch
            print(f"Progress: {last_id} rows processed...")

# %% [code cell 4]
def process_and_upload(engine, table_name, output_table):
    preprocessor = Preprocessor()
    batch_size = 1000
    cols_to_explode = ['DocContent']

    # Initialize the generator
    data_stream = stream_data(engine, table_name, batch_size)

    while True:
        # 1. Collect a batch of rows from the generator
        batch_rows = []
        try:
            for _ in range(batch_size):
                batch_rows.append(next(data_stream))
        except StopIteration:
            pass # End of data reached

        if not batch_rows:
            break

        # 2. Convert batch to DataFrame
        df = pd.DataFrame(batch_rows)

        # 3. Apply your Processing Logic
        # Note: Ensure DocContent is handled safely
        df["DocContent"] = df["DocContent"].apply(
            lambda x: preprocessor.process_xml(x) if pd.notnull(x) else None
        )
        df["DocContent"] = df["DocContent"].apply(
            lambda x: preprocessor.convert_json_to_json(x) if pd.notnull(x) else None
        )

        # 4. Explode the data
        df_exploded = preprocessor.explode_json_categories_all_columns(df, cols_to_explode)

        # 5. Insert back into MySQL
        # 'append' adds to the table; 'replace' would drop it every time
        df_exploded.to_sql(output_table, con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
        
        print(f"Successfully processed and inserted a batch of {len(df_exploded)} exploded rows.")

        if len(batch_rows) < batch_size:
            break # Exit if the last batch was smaller than batch_size

# %% [code cell 5]
if __name__ == "__main__":
    engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/primerecord_bin")
    table_name = "clinicalbin_xml_decrypt_26"
    output_table = "clinicalbin_xml_processed_26"
    process_tables_mysql(engine, table_name)
    process_and_upload(engine, table_name, output_table)
