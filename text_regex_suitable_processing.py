import re
import pandas as pd

entity_unit_map = {
    'width': {'centimetre', 'foot', 'inch', 'metre', 'millimetre', 'yard'},
    'depth': {'centimetre', 'foot', 'inch', 'metre', 'millimetre', 'yard'},
    'height': {'centimetre', 'foot', 'inch', 'metre', 'millimetre', 'yard'},
    'item_weight': {'gram',
        'kilogram',
        'microgram',
        'milligram',
        'ounce',
        'pound',
        'ton'},
    'maximum_weight_recommendation': {'gram',
        'kilogram',
        'microgram',
        'milligram',
        'ounce',
        'pound',
        'ton'},
    'voltage': {'kilovolt', 'millivolt', 'volt'},
    'wattage': {'kilowatt', 'watt'},
    'item_volume': {'centilitre',
        'cubic foot',
        'cubic inch',
        'cup',
        'decilitre',
        'fluid ounce',
        'gallon',
        'imperial gallon',
        'litre',
        'microlitre',
        'millilitre',
        'pint',
        'quart'}
}

# Load the CSV file
df = pd.read_csv('C:/Users/parth/Desktop/amazon-ml-challenege-dataset/student_resource/amazon-ml-challenge-2024/samarth/filtered_extracted_text.csv')

# Define unit mappings globally
unit_mappings = {
    'mm': 'millimeter', 'millimeter': 'millimeter',
    'cm': 'centimeter', 'centimeter': 'centimeter',
    'ft': 'foot', 'foot': 'foot',
    'in': 'inch', 'inch': 'inch', '""': 'inch', 'inches': 'inch',
    'm': 'meter', 'meter': 'meter',
    'kg': 'kilogram', 'kilogram': 'kilogram',
    'g': 'gram', 'gram': 'gram', 'gm': 'gram',
    'lb': 'pound', 'pound': 'pound',
    'oz': 'ounce', 'ounce': 'ounce',
    'mcg': 'microgram', 'microgram': 'microgram',
    'mg': 'milligram', 'milligram': 'milligram',
    'ton': 'ton', 'kv': 'kilovolt', 'mv': 'millivolt',
    'v': 'volt', 'w': 'watt', 'kw': 'kilowatt','W': 'watt', 'V': 'volt',
    'cl': 'centiliter', 'l': 'liter', 'ml': 'milliliter',
    'µl': 'microliter', 'pt': 'pint', 'qt': 'quart',
    'cup': 'cup', 'gal': 'gallon', 'fl oz': 'fluid ounce'
}

# Function to create a regex pattern to match unit abbreviations, including ranges and special characters
def replace_units(entity_name, text):
    # Convert the text to string (in case there are non-string values like NaN)
    if not isinstance(text, str):
        return text  # If it's not a string, return as-is

    # Fetch the allowed units for the entity
    allowed_units = entity_unit_map.get(entity_name, set())
    
    # Create a regex pattern to match ranges, units, and handle special cases
    pattern = re.compile(r'(\d+\.?\d*(?:-\d+\.?\d*)?)\s*(' + '|'.join(re.escape(unit) for unit in allowed_units) + r'|""|inches|inch\b)', flags=re.IGNORECASE)

    # Replace all abbreviations with the full form and ensure a space between the number and the unit
    def replacer(match):
        number_part = match.group(1).strip()  # Capture and strip any spaces around the number part (group 1)
        abbr = match.group(2).lower()  # Capture the abbreviation part (group 2)

        # Ensure there's a space between the number and the full unit name
        return number_part + ' ' + unit_mappings.get(abbr, abbr)
    
    # Apply the replacement to the text
    text = pattern.sub(replacer, text)
    
    # Handle special cases with special characters
    text = re.sub(r'(\d+\.?\d*)\s*(cm|in|inch|""|inches)', lambda m: m.group(1) + ' ' + unit_mappings.get(m.group(2), m.group(2)), text)
    
    # Replace 'inchch' with 'inch'
    text = re.sub(r'inchch\b', 'inch', text)
    
    text = re.sub(r'inchchch\b', 'inch', text)
    
    return text

# Apply the function to the extracted_text column
df['extracted_text'] = df.apply(lambda row: replace_units(row['entity_name'], row['extracted_text']), axis=1)

# Save the modified dataframe back to a CSV
df.to_csv('updates_filtered_extracted_text.csv', index=False)

print("Abbreviations replaced, ranges and special cases handled successfully.")
