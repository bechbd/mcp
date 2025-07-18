# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
from typing import Dict, List, Set


def remove_null_values(d: Dict):
    """Return a new dictionary with the key-value pair of any null value removed."""
    return {k: v for k, v in d.items() if v}


def filter_by_prefixes(strings: Set[str], prefixes: Set[str]) -> Set[str]:
    """Return strings filtered down to only those that start with any of the prefixes."""
    return {s for s in strings if any(s.startswith(p) for p in prefixes)}


def epoch_ms_to_utc_iso(ms: int) -> str:
    """Convert milliseconds since epoch to an ISO 8601 timestamp string."""
    # Use replace to convert 'Z' suffix to '+00:00' for compatibility with fromisoformat()
    iso_string = datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc).isoformat()
    # Ensure the timezone is represented as +00:00 instead of +00:00 (if it's already that way)
    # or convert Z to +00:00 if the isoformat() method ever returns Z
    if iso_string.endswith('Z'):
        iso_string = iso_string[:-1] + '+00:00'
    return iso_string


def clean_up_pattern(pattern_result: List[Dict[str, str]]):
    """Clean up results from an @pattern query to remove extra fields and limit the log samples to 1.

    The main purpose of this is to keep the token usage down because of the potential for results to
    exceed the context window size.
    """
    for entry in pattern_result:
        entry.pop('@tokens', None)
        entry.pop('@visualization', None)
        # limit to 1 sample
        entry['@logSamples'] = json.loads(entry.get('@logSamples', '[]'))[:1]
