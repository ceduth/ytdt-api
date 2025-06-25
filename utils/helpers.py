import os
import csv
import pathlib
from bidict import bidict


__all__ = (
  'file_exists', 'remove_file', 'map_language', 'parse_locale', 'bidirectional_lookup',
  'load_video_ids_from_csv', 'split_kwargs'
)



file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)

# Extract language and country codes from locale code eg. 'en-US', 'en_US'
parse_locale = lambda code: (*((code or '').replace('_', '-').split('-') + [None]),)[:2]


def bidirectional_lookup(mapping: dict, key_or_value: str, raise_exc=True):

  bimap = bidict(mapping)
  if key_or_value in bimap:
    return bimap[key_or_value]
  elif key_or_value in bimap.inv:
    return bimap.inv[key_or_value]
  else:
    if not raise_exc:
      return "Unknown"
    raise KeyError(f"{key_or_value} not found in either direction")
  

# Attempt to map language name to code (very basic)
map_language = lambda lang: bidirectional_lookup({
  "English": "en",
  "Spanish": "es",
  "French": "fr",
  "German": "de",
  "Chinese": "zh",
  "Japanese": "ja"
}, lang)




def load_video_ids_from_csv(path: str, column: str) -> list[str]:
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return [row[column].strip() for row in reader if row.get(column)]
    

def split_kwargs(keys_to_extract, kwargs):
    extracted = {k: kwargs[k] for k in keys_to_extract if k in kwargs}
    rest = {k: v for k, v in kwargs.items() if k not in keys_to_extract}
    return extracted, rest