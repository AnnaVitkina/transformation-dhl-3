"""
MainCosts data transformation for the DHL rate-card pipeline.

This module contains all the logic for converting the raw MainCosts JSON data
into the structured rows that get written to the MainCosts Excel tab.

The MainCosts tab is the most complex tab in the workbook.  It shows one row per
"lane" (a unique combination of service type + zone), with all cost categories
(Envelope, Documents, Parcels …) merged into a single wide row.

Functions (public):
  pivot_main_costs                  – legacy flat pivot (kept for reference, not used in main flow)
  build_matrix_main_costs           – builds the lane-based matrix view (main flow)
  expand_main_costs_lanes_by_zoning – replaces letter zones with real O/D pairs
  sort_main_costs_rows_for_layout   – final Excel row order: matrix rows by Origin zone index

Helper functions (private, prefixed with _):
  _zone_has_letters
  _zone_sort_key
  global_country
  parse_zoning_matrix
  _matrix_zone_to_letter
  _main_words
  _find_matrix_for_service
"""

import re
from collections import defaultdict

from transform_other_tabs import build_zone_label_lookup


# ---------------------------------------------------------------------------
# Weight sorting helper
# ---------------------------------------------------------------------------

def _weight_sort_key(w):
    """
    Sort key for weight breakpoint values so they always appear in correct
    numeric order regardless of how they were stored as strings.

    Numeric values (e.g. "0.5", "1", "10.0") are sorted as floats:
        0.5 → 1.0 → 1.5 → 2.0 → 10.0 → 11.0  (correct)
    Non-numeric values (rare edge cases) are sorted alphabetically after
    all numeric values.

    Examples:
        sorted(["10.0", "2.0", "0.5", "1.0"], key=_weight_sort_key)
        → ["0.5", "1.0", "2.0", "10.0"]
    """
    try:
        return (0, float(w))   # numeric: sort by float value
    except (ValueError, TypeError):
        return (1, str(w))     # non-numeric: sort alphabetically after numbers


# ---------------------------------------------------------------------------
# Zone-name helpers
# ---------------------------------------------------------------------------

def _zone_has_letters(zone_name):
    """
    Check whether a zone name uses a letter identifier (e.g. "Zone A") rather than
    a number identifier (e.g. "Zone 1").

    Returns True for "Zone A", "Zone E", etc.
    Returns False for "Zone 1", "Zone 12", etc.
    """
    s = (zone_name or '').strip()
    if not s:
        return False
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    return any(c.isalpha() for c in suffix)


def _zone_is_single_letter(zone_name):
    """
    Return True only when the zone identifier is exactly one letter (e.g. "Zone A", "B").

    This is the fallback criterion used when no matching ZoningMatrix exists for a service.
    A single-letter zone almost certainly refers to a matrix lookup code even when the
    matrix name doesn't match the service name closely enough to be found automatically.

    Examples:
      "Zone A"  -> True   (single letter after "Zone ")
      "Zone AB" -> False  (two letters – probably a real zone name, not a matrix code)
      "Zone 1"  -> False  (number, not a letter)
      "A"       -> True   (bare single letter)
    """
    s = (zone_name or '').strip()
    if not s:
        return False
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    # Exactly one alphabetic character and nothing else
    return len(suffix) == 1 and suffix.isalpha()


def _zone_needs_matrix_lookup(zone_name, service_type, zoning_lookup):
    """
    Decide whether a zone in a given service should be treated as a matrix lookup code
    (i.e. needs to be expanded into real Origin/Destination pairs via the ZoningMatrix).

    NEW TWO-STEP LOGIC:

    Step 1 – Service-matrix match (primary):
      Try to find a ZoningMatrix whose name corresponds to this service type.
      If a match is found, ALL zones for this service are matrix zones – regardless
      of whether their name contains letters or numbers.
      This handles the common case where service "DHL EXPRESS WORLDWIDE THIRD COUNTRY"
      has a matching matrix "DHL EXPRESS THIRD COUNTRY ZONE MATRIX".

    Step 2 – Single-letter fallback:
      If no matrix was found for this service, check whether the zone identifier is
      exactly one letter (e.g. "A", "B", "E").  A bare single letter almost certainly
      means the zone is a matrix lookup code even when the matrix name couldn't be
      matched automatically.

    Returns True if the zone should be flagged as a matrix zone, False otherwise.
    """
    if not zone_name:
        return False

    # Step 1: does a matrix exist for this service?
    if zoning_lookup and _find_matrix_for_service(zoning_lookup, service_type):
        # A matching matrix was found – this zone belongs to it
        return True

    # Step 2: no matrix found for the service; fall back to single-letter check
    return _zone_is_single_letter(zone_name)


def _dhl_express_domestic_single_cost_zone_only(main_costs):
    """
    True if MainCosts contains DHL EXPRESS DOMESTIC pricing and every non-adder section
    for that service uses exactly one distinct zone name (e.g. only "Zone A").

    Then we skip matrix expansion and keep a single lane (see PASS 2;
    expand_main_costs_lanes_by_zoning skips empty Matrix zone).

    Origin/Destination are then filled with either the CountryZoning short label
    (``DOMESTIC_ZONE_*``) when a **DHL EXPRESS DOMESTIC ZONING** block exists in
    CountryZoning, or else the carrier country — see PASS 2 closing steps.
    """
    zones = set()
    seen_domestic = False
    for rate_card in main_costs:
        if _is_adder_section(rate_card):
            continue
        st = (rate_card.get('service_type') or '').strip()
        if st.upper() != 'DHL EXPRESS DOMESTIC':
            continue
        seen_domestic = True
        for zname in (rate_card.get('zone_headers') or {}).values():
            zname = (zname or '').strip()
            if zname:
                zones.add(zname)
    if not seen_domestic:
        return False
    return len(zones) == 1


_DOMESTIC_ZONING_RATE_PHRASE = 'DHL EXPRESS DOMESTIC ZONING'


def _country_zoning_has_dhl_express_domestic_zoning(country_zoning):
    """
    True if CountryZoning contains a **DHL EXPRESS DOMESTIC ZONING** rate block
    (contiguous phrase), i.e. not only ``... DOMESTIC THIRD COUNTRY ZONING``.
    Used with single-zone domestic to fill Origin/Destination from CountryZoning labels.
    """
    if not country_zoning:
        return False
    needle = _DOMESTIC_ZONING_RATE_PHRASE
    for item in country_zoning:
        if not isinstance(item, dict):
            continue
        rn = (item.get('RateName') or '').strip()
        if not rn:
            continue
        norm = ' '.join(rn.split()).upper()
        if needle in norm:
            return True
    return False


def _domestic_zone_short_label(zone_label_lookup, zone_name):
    """
    Return label like ``DOMESTIC_ZONE_A`` for MainCosts zone column (e.g. ``Zone A``)
    using the same (prefix, zone) keys as apply_zone_labels_to_main_costs.
    """
    if not zone_label_lookup or not zone_name:
        return None
    zn = re.sub(r'(?i)^zone\s*', '', str(zone_name).strip()).strip()
    if not zn:
        return None
    return zone_label_lookup.get(('DOMESTIC_ZONE', zn))


def _zone_sort_key(zone_name):
    """
    Generate a sort key for a zone name so that zones appear in a sensible order:
    numeric zones first (Zone 1, Zone 2, Zone 10 …) then letter/other zones after.

    Without this, alphabetical sorting would give: Zone 1, Zone 10, Zone 2 (wrong).
    With this, we get: Zone 1, Zone 2, Zone 10, Zone A (correct).

    Returns a tuple (group, value) where:
      group=0 means numeric zone (sorted by number)
      group=1 means letter/other zone (sorted after all numeric zones)
    """
    s = (zone_name or '').strip()
    if not s:
        return (1, 0)
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    try:
        return (0, float(suffix))
    except (ValueError, TypeError):
        return (1, suffix)   # sort non-numeric zones alphabetically within group 1


def global_country(metadata):
    """
    Extract the country name from the carrier string in the metadata.

    DHL carrier names follow the pattern "DHL Express <Country>" (case-insensitive),
    e.g. "DHL Express France"  -> "France"
         "DHL EXPRESS GERMANY" -> "Germany"
         "DHL express Netherlands" -> "Netherlands"

    The country is everything that comes after the words "DHL" and "EXPRESS"
    (or "EXPRESS" alone), title-cased for consistency.

    If the pattern is not found, the last word of the carrier string is used
    as a fallback so the field is never left empty when a carrier is present.

    This country name is used to fill in the Origin or Destination column for:
    - Domestic lanes (both Origin and Destination = carrier's country)
    - Non-zoned export lanes (Destination = carrier's country)
    - Non-zoned import lanes (Origin = carrier's country)
    """
    import re
    carrier = (metadata.get('carrier') or '').replace('\n', ' ').strip()
    if not carrier:
        return ''

    # Words that signal the end of the country name (non-country suffixes)
    _STOP_WORDS = {
        'customer', 'customers', 'services', 'service', 'surcharges', 'surcharge',
        'export', 'import', 'domestic', 'rates', 'rate', 'ratecard', 'tariff',
        'tariffs', 'zone', 'zones', 'express', 'dhl', 'international', 'standard',
        'priority', 'economy', 'freight', 'air', 'ground', 'parcel', 'and',
    }

    # Match everything after "DHL EXPRESS", then walk word by word until a stop word
    m = re.search(r'\bDHL\s+EXPRESS?\s+(.+)', carrier, re.IGNORECASE)
    if m:
        remainder = m.group(1).strip()
        country_words = []
        for word in remainder.split():
            if word.lower() in _STOP_WORDS:
                break
            country_words.append(word)
        if country_words:
            return ' '.join(country_words).title()   # e.g. "UNITED KINGDOM" -> "United Kingdom"

    # Fallback: return the last word of the carrier string
    parts = carrier.split()
    return parts[-1].title() if parts else ''


# ---------------------------------------------------------------------------
# ZoningMatrix parsing and lane expansion
# ---------------------------------------------------------------------------

def parse_zoning_matrix(zoning_matrix):
    """
    Read the ZoningMatrix data and build a lookup table that answers the question:
    "For zone letter A in matrix X, which (origin zone, destination zone) pairs exist?"

    BACKGROUND – what is a ZoningMatrix?
    The ZoningMatrix is a grid that maps pairs of origin and destination zone numbers
    to a single letter (A, B, C …).  For example:
        Origin 1 -> Destination 3 -> letter "A"
        Origin 2 -> Destination 3 -> letter "A"
        Origin 1 -> Destination 5 -> letter "E"

    The MainCosts pricing table uses those letters as shorthand: instead of listing
    a price for every individual origin/destination pair, it lists one price per letter.
    This function reverses the matrix so we can later expand each letter back into
    all the concrete (origin, destination) pairs it represents.

    THE JSON STRUCTURE:
    The ZoningMatrix arrives as a flat list of rows.  Two types of rows alternate:
      - Header row: has 'MatrixName' filled in + DestinationZone1, DestinationZone2 …
                    whose values are the destination zone numbers (1, 2, 3 …)
      - Data row:   has 'OriginZone' filled in + DestinationZone1, DestinationZone2 …
                    whose values are the zone letters (A, B, E …)

    WHAT THIS FUNCTION RETURNS:
    A dictionary where:
      key   = (matrix_name, zone_letter)   e.g. ("DHL EXPRESS WW ZONE MATRIX", "A")
      value = list of (origin_zone, destination_zone) pairs  e.g. [("1", "3"), ("2", "3")]
    """
    result = {}                    # the lookup table we are building
    dest_cols = None               # ordered list of "DestinationZone1", "DestinationZone2" … keys
    header_dest_nums = None        # the actual destination zone numbers read from the header row
    current_matrix_name = None     # name of the matrix block we are currently inside

    for row in zoning_matrix or []:
        matrix_name = (row.get('MatrixName') or '').strip()
        origin_zone = (row.get('OriginZone') or '').strip()

        # Find DestinationZone* keys in this row (may be in same row as MatrixName or in next row)
        dest_keys = sorted(
            [k for k in row if re.match(r'^DestinationZone\d+$', k)],
            key=lambda k: int(re.search(r'\d+', k).group())
        )

        if matrix_name:
            # ---------------------------------------------------------------
            # This is a HEADER ROW – it starts a new matrix block.
            # Example: MatrixName="DHL EXPRESS WW ZONE MATRIX",
            #          DestinationZone1="1", DestinationZone2="2", DestinationZone3="3"
            # Some PDFs put MatrixName alone in one row; then the next row has the zone columns.
            # ---------------------------------------------------------------
            current_matrix_name = matrix_name

            if dest_keys:
                dest_cols = dest_keys
                header_dest_nums = [str(row.get(k, '')).strip() for k in dest_cols]
            # else: keep previous dest_cols/header_dest_nums so data rows can still be parsed
            continue   # move on to the next row (this header row has no zone letters to add)

        if current_matrix_name and dest_keys and not origin_zone:
            # Row has DestinationZone* but no OriginZone – treat as secondary header (zone column numbers)
            # so the first matrix is not skipped when its header is split across two rows
            dest_cols = dest_keys
            header_dest_nums = [str(row.get(k, '')).strip() for k in dest_cols]
            continue

        if current_matrix_name and origin_zone and dest_cols:
            # ---------------------------------------------------------------
            # This is a DATA ROW – it belongs to the current matrix block.
            # Example: OriginZone="1",
            #          DestinationZone1="A", DestinationZone2="A", DestinationZone3="E"
            # This means: origin 1 -> destination 1 = letter A
            #             origin 1 -> destination 2 = letter A
            #             origin 1 -> destination 3 = letter E
            # ---------------------------------------------------------------
            for col_idx, dest_key in enumerate(dest_cols):
                if col_idx >= len(header_dest_nums):
                    continue   # safety check: don't go past the number of header columns
                dest_zone_num = header_dest_nums[col_idx]   # e.g. "3"
                if not dest_zone_num:
                    continue   # skip if the header had no zone number for this column
                cell_letter = (row.get(dest_key) or '').strip()   # e.g. "A"
                if not cell_letter:
                    continue   # skip empty cells (no zone letter assigned)

                # Build the lookup key: (matrix_name, letter)
                key = (current_matrix_name, cell_letter.upper())
                if key not in result:
                    result[key] = []   # create a new list for this letter if first time seen
                # Record that this (origin, destination) pair maps to this letter
                result[key].append((origin_zone, dest_zone_num))

    return result


def _matrix_zone_to_letter(matrix_zone):
    """
    Extract just the letter part from a zone name like "Zone E" -> "E".
    This is needed because the lookup table is keyed by the letter alone, not the full name.
    If the input is already just a letter (no "Zone " prefix), it is returned as-is in uppercase.
    """
    s = (matrix_zone or '').strip()
    if not s:
        return ''
    if s.upper().startswith('ZONE '):
        return s[5:].strip().upper()   # remove "Zone " and return the rest in uppercase
    return s.upper()


def _main_words(text):
    """
    Split a text string into its meaningful words (all uppercase), ignoring the
    generic words "ZONE" and "MATRIX" which appear in almost every matrix name
    and would cause false matches.

    Example: "DHL EXPRESS THIRD COUNTRY ZONE MATRIX" -> {"DHL", "EXPRESS", "THIRD", "COUNTRY"}
    """
    if not text:
        return set()
    words = set((text or '').upper().split())
    words.discard('ZONE')     # too generic to be useful for matching
    words.discard('MATRIX')   # too generic to be useful for matching
    return words


def _norm_matrix_name(s):
    """Normalize matrix/service strings for comparison (collapse whitespace, upper)."""
    return ' '.join((s or '').strip().split()).upper()


def _find_matrix_name_in_lookup(canonical_name, matrix_names):
    """Return the actual matrix name from lookup if it matches canonical (spacing/case tolerant)."""
    want = _norm_matrix_name(canonical_name)
    for mn in matrix_names:
        if _norm_matrix_name(mn) == want:
            return mn
    return None


def _explicit_third_country_matrix(service_upper, matrix_names):
    """
    Fixed mapping: MainCosts service line -> ZoningMatrix name.

    Without this, Attempt 0 returned the first non-domestic third-country matrix from an
    unordered set — often DHL ECONOMY SELECT THIRD COUNTRY ZONE MATRIX before DHL EXPRESS
    THIRD COUNTRY ZONE MATRIX, so WORLDWIDE THIRD COUNTRY lanes used the wrong grid.

    Mapping (only if that matrix exists in zoning_lookup):
      DHL EXPRESS WORLDWIDE THIRD COUNTRY  -> DHL EXPRESS THIRD COUNTRY ZONE MATRIX
      DHL EXPRESS DOMESTIC THIRD COUNTRY     -> DHL EXPRESS DOMESTIC THIRD COUNTRY ZONE MATRIX
      DHL ECONOMY SELECT THIRD COUNTRY       -> DHL ECONOMY SELECT THIRD COUNTRY ZONE MATRIX
    """
    if 'THIRD' not in service_upper or 'COUNTRY' not in service_upper:
        return None
    if 'ECONOMY' in service_upper and 'SELECT' in service_upper:
        return _find_matrix_name_in_lookup(
            'DHL ECONOMY SELECT THIRD COUNTRY ZONE MATRIX', matrix_names
        )
    if 'DOMESTIC' in service_upper:
        return _find_matrix_name_in_lookup(
            'DHL EXPRESS DOMESTIC THIRD COUNTRY ZONE MATRIX', matrix_names
        )
    if 'WORLDWIDE' in service_upper:
        return _find_matrix_name_in_lookup(
            'DHL EXPRESS THIRD COUNTRY ZONE MATRIX', matrix_names
        )
    return None


def _find_matrix_for_service(zoning_lookup, service):
    """
    Given a service type name (e.g. "DHL EXPRESS THIRD COUNTRY"), find which matrix
    in the zoning_lookup corresponds to it.

    WHY THIS IS NEEDED:
    The service names in MainCosts and the matrix names in ZoningMatrix are written
    slightly differently.  For example:
      - Service:  "DHL EXPRESS THIRD COUNTRY"
      - Matrix:   "DHL EXPRESS THIRD COUNTRY ZONE MATRIX"
    We need to match them up despite these differences.

    MATCHING STRATEGY (tries each approach in order, returns the first match found):
      0. Explicit third-country service -> matrix (_explicit_third_country_matrix)
      1. Attempt 0 legacy: WORLDWIDE THIRD COUNTRY non-domestic matrices
      2. Direct substring: does the service name appear inside the matrix name, or vice versa?
      3. Strip " ZONE MATRIX" from the matrix name, then try substring again.
      4. Word-level match: do all meaningful words from the matrix name appear in the service?
         e.g. {"DHL", "EXPRESS", "THIRD", "COUNTRY"} are all present in "DHL EXPRESS THIRD COUNTRY"

    Returns the matching matrix name, or None if no match is found.
    """
    service = (service or '').strip()
    if not service:
        return None
    service_upper = service.upper()
    service_words = _main_words(service)

    # Get all unique matrix names from the lookup (ignoring the zone letter part of each key)
    matrix_names = {mn for (mn, _) in zoning_lookup}

    explicit = _explicit_third_country_matrix(service_upper, matrix_names)
    if explicit:
        return explicit

    # --- Attempt 0: WORLDWIDE THIRD COUNTRY must use the non-Domestic matrix ---
    # Service "DHL EXPRESS WORLDWIDE THIRD COUNTRY" -> "DHL EXPRESS THIRD COUNTRY ZONE MATRIX"
    # (not "DHL EXPRESS DOMESTIC THIRD COUNTRY ZONE MATRIX"). Prefer matrix that has THIRD COUNTRY but not DOMESTIC.
    if 'WORLDWIDE' in service_upper and 'THIRD' in service_upper and 'COUNTRY' in service_upper:
        for mn in matrix_names:
            mn_upper = mn.upper()
            if 'THIRD' in mn_upper and 'COUNTRY' in mn_upper and 'DOMESTIC' not in mn_upper:
                return mn
        # Fallback: source data often has only DOMESTIC THIRD COUNTRY ZONE MATRIX; use it for WORLDWIDE so expansion runs
        for mn in matrix_names:
            mn_upper = mn.upper()
            if 'THIRD' in mn_upper and 'COUNTRY' in mn_upper:
                return mn

    # --- Attempt 1: direct substring match ---
    for mn in matrix_names:
        if service in mn or mn in service:
            return mn   # found a match, return immediately

    # --- Attempt 2: strip the " ZONE MATRIX" boilerplate and try again ---
    for mn in matrix_names:
        normalized = mn.replace(' ZONE MATRIX', '').strip()
        if service in normalized or normalized in service:
            return mn

    # --- Attempt 3: all meaningful words from the matrix name must be in the service ---
    # This handles cases where word order differs or extra words are present
    for mn in matrix_names:
        matrix_words = _main_words(mn.replace(' ZONE MATRIX', ''))
        # "<=" on sets means "is a subset of": all matrix words appear in service words
        if matrix_words and matrix_words <= service_words:
            return mn

    return None   # no match found in any of the three attempts


# ---------------------------------------------------------------------------
# MainCosts – legacy flat pivot (zones as rows, weights as columns)
# ---------------------------------------------------------------------------

def pivot_main_costs(main_costs, metadata):
    """
    (Legacy / unused view) Convert the MainCosts pricing data into a simple flat table
    where each row = one delivery zone, and each column = one weight bracket.

    Example of what the output looks like:
        Zone    | 0.5 KG | 1 KG | 2 KG
        Zone 1  |  12.50 | 15.00| 18.00
        Zone 2  |  14.00 | 17.50| 21.00

    This is an older, simpler view.  The main view used today is build_matrix_main_costs().
    """
    rows = []   # will hold all the output rows we build

    # Pull the three identity fields that appear on every row
    client = (metadata.get('client') or '')
    carrier = (metadata.get('carrier') or '').replace('\n', ' ')  # remove any line breaks
    validity_date = (metadata.get('validity_date') or '')

    # Loop over each "rate card" block in the MainCosts list.
    # Each rate card covers one service type (e.g. "DHL EXPRESS WORLDWIDE EXPORT")
    # and one cost category (e.g. "Documents").
    for section_idx, rate_card in enumerate(main_costs, 1):
        service_type = rate_card.get('service_type') or ''
        cost_category = rate_card.get('cost_category', '')
        weight_unit = rate_card.get('weight_unit', 'KG')

        # zone_headers maps internal short keys (e.g. "Z1") to display names (e.g. "Zone 1")
        zone_headers = rate_card.get('zone_headers', {})

        # pricing is a list where each entry covers one weight breakpoint.
        # Example entry: { "weight": "0.5", "zone_prices": {"Z1": 12.50, "Z2": 14.00} }
        pricing = rate_card.get('pricing', [])

        # ---------------------------------------------------------------
        # Step 1: Reorganise the data from "weight-first" to "zone-first".
        # ---------------------------------------------------------------
        zone_price_matrix = {}   # zone_name -> { weight -> price }
        weights_set = set()      # collect all unique weight values seen

        for price_entry in pricing:
            weight = price_entry.get('weight', '')
            weights_set.add(weight)
            zone_prices = price_entry.get('zone_prices', {})

            for zone_key, price in zone_prices.items():
                zone_name = zone_headers.get(zone_key, zone_key)
                if zone_name not in zone_price_matrix:
                    zone_price_matrix[zone_name] = {}
                zone_price_matrix[zone_name][weight] = price

        # Sort the weight values numerically
        weights_sorted = sorted(weights_set, key=_weight_sort_key)

        # ---------------------------------------------------------------
        # Step 2: Build one output row per zone.
        # ---------------------------------------------------------------
        for zone_name, weight_prices in zone_price_matrix.items():
            row = {
                'Client': client,
                'Carrier': carrier,
                'Validity Date': validity_date,
                'Section': section_idx,
                'Service Type': service_type,
                'Cost Category': cost_category,
                'Weight Unit': weight_unit,
                'Zone': zone_name
            }

            for weight in weights_sorted:
                col_name = f"{weight} {weight_unit}"   # e.g. "0.5 KG"
                row[col_name] = weight_prices.get(weight, '')

            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
def _format_cost_category(raw_name):
    """
    Wrap a raw cost-category name in the standard "Transport cost (...)" label.

    Examples:
        "Documents up to 2.0 KG"  ->  "Transport cost (Documents up to 2.0 KG)"
        "Envelope up to 300 g"    ->  "Transport cost (Envelope up to 300 g)"
        ""                        ->  ""   (empty stays empty)

    Note: "Adder rate per additional X KG from Y" sections are not formatted
    as a separate cost; they are merged into the previous category (see
    _is_adder_section and adder handling in build_matrix_main_costs).
    """
    raw_name = (raw_name or '').strip()
    if not raw_name:
        return raw_name
    return f"Transport cost ({raw_name})"


def _is_adder_section(rate_card):
    """
    Return True if this rate card is an "adder" table that should be merged
    into the previous cost category instead of creating a new one.

    Adder tables have cost_category like:
      "Adder rate per additional 0.5 KG from 10.1 KG"
      "Adder rate per additional 1 KG from 30.1 KG"
    and weight values like "10.1\n20" (From/To range).
    """
    cost_category = (rate_card.get('cost_category') or '').strip()
    if not cost_category:
        return False
    cost_lower = cost_category.lower()
    return 'adder rate' in cost_lower and 'additional' in cost_lower


def _parse_adder_unit(cost_category_raw):
    """
    Extract the unit value from an adder cost category for the "p/X unit" label.

    Examples:
        "Adder rate per additional 0.5 KG from 10.1 KG"  ->  "0.5"
        "Adder rate per additional 1 KG from 30.1 KG"    ->  "1"
    """
    s = (cost_category_raw or '').strip()
    # Match "additional" followed by optional spaces and a number (int or decimal)
    m = re.search(r'additional\s+([0-9]+(?:\.[0-9]+)?)\s*(?:KG|kg|g)?', s, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return '1'


def _normalize_adder_weight(weight_str):
    """
    Convert adder weight range from extracted form to display form.

    The extracted value is often "10.1\\n20" (From\\nTo). We display as "10.1-20".
    """
    if not weight_str:
        return weight_str
    s = str(weight_str).strip().replace('\n', '-').replace('\r', '')
    # Collapse multiple spaces or dashes into one dash
    s = re.sub(r'[\s\-]+', '-', s).strip('-')
    return s if s else weight_str


def _adder_range_sort_key(range_str):
    """
    Sort key for adder weight ranges (e.g. "10.1-20", "70.1-100") so they appear
    in increasing order by the range start. "10.1-20" < "20.1-30" < "70.1-100".
    """
    if not range_str:
        return (0, 0.0)
    s = str(range_str).strip()
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)', s)
    if m:
        try:
            return (0, float(m.group(1)))
        except ValueError:
            pass
    return (1, s)


def _adder_block_sort_key(block):
    """
    Sort key for category blocks so that Flat (main) is first, then adder blocks
    by unit: p/0.5 unit, p/1 unit, p/5 unit (by numeric value).
    """
    weight_unit, weights, row4_label = block
    if row4_label == 'Flat':
        return (0, 0.0)
    m = re.search(r'p/([0-9]+(?:\.[0-9]+)?)\s*unit', weight_unit, re.IGNORECASE)
    if m:
        try:
            return (1, float(m.group(1)))
        except ValueError:
            pass
    return (1, 999.0)


def _range_start_value(weight_str):
    """
    Parse the start (first number) from a range weight like "30.1-70" or "70.1-300".
    Returns float or None if not a range.
    """
    if not weight_str:
        return None
    s = str(weight_str).strip()
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)\s*[-–]\s*', s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


# When mixed adder units exist (e.g. p/0.5 then p/1), flat weights are trimmed at the
# *start* of the first range of the *last* unit's block (e.g. 30.1 from "30.1-70"), not
# at the end of that range — so flat stops at <=30 kg, not <=70 kg.


def _first_adder_range_start(weights):
    """Start (kg) of the first From–To range after sorting; None if not a range."""
    if not weights:
        return None
    r0 = sorted(weights, key=_adder_range_sort_key)[0]
    return _range_start_value(r0)


def _flat_trim_threshold_for_category_blocks(blocks):
    """
    Decide the max flat weight to keep for a merged cost category.

    - If all adder blocks share the same p/X unit: use the *minimum* range *start* across
      every adder range (unchanged).
    - If adder units differ (e.g. p/0.5 then p/1): use the *start* of the first range in
      the *first* block whose p/X matches the *last* block (handover to the final adder
      tier), e.g. 30.1 -> keep flat <= 30, not <= 70.
    """
    if not blocks or blocks[0][2] != 'Flat':
        return None
    adders = blocks[1:]
    if not adders:
        return None
    distinct_units = {b[2] for b in adders}
    if len(distinct_units) == 1:
        min_start = None
        for _wu, weights, _rl in adders:
            for w in weights:
                st = _range_start_value(w)
                if st is not None:
                    if min_start is None or st < min_start:
                        min_start = st
        return min_start
    U_last = adders[-1][2]
    for _wu, weights, rl in adders:
        if rl == U_last:
            return _first_adder_range_start(weights)
    return None


def _threshold_from_ordered_adder_blocks(ordered):
    """
    ordered: list of (rate_by_str, list of normalized weight strings) in document order.
    Same rules as _flat_trim_threshold_for_category_blocks but keyed by service.
    """
    if not ordered:
        return None
    labels = [x[0] for x in ordered]
    if len(set(labels)) == 1:
        min_start = None
        for _rb, weights in ordered:
            for w in weights:
                st = _range_start_value(w)
                if st is not None:
                    if min_start is None or st < min_start:
                        min_start = st
        return min_start
    U_last = labels[-1]
    for rb, weights in ordered:
        if rb == U_last:
            return _first_adder_range_start(weights)
    return None


# Max flat (kg) >= (start of last-tier adder first range) - HANDOVER_TOLERANCE => drop
# intermediate adder columns (e.g. p/0.5) so <=20 / <=30 p/0.5 do not sit beside flat.
_HANDOVER_TOLERANCE_KG = 0.25


def _drop_intermediate_adder_blocks(category_specs):
    """
    If multiple adder unit types exist and flat weights already reach the handover to the
    final tier (e.g. flat max 30 kg and p/1 starts at 30.1 kg), remove non-final adder
    blocks (e.g. entire p/0.5 block) so those columns are not shown alongside flat.
    """
    for _cat_name, blocks in category_specs:
        if not blocks or blocks[0][2] != 'Flat':
            continue
        adders = blocks[1:]
        if len(adders) < 2:
            continue
        if len({b[2] for b in adders}) < 2:
            continue
        U_last = adders[-1][2]
        s_u = None
        for _wu, weights, rl in adders:
            if rl == U_last:
                s_u = _first_adder_range_start(weights)
                break
        if s_u is None:
            continue
        try:
            max_flat = max(float(w) for w in blocks[0][1])
        except (ValueError, TypeError):
            continue
        if max_flat < s_u - _HANDOVER_TOLERANCE_KG:
            continue
        new_blocks = [blocks[0]]
        for b in adders:
            if b[2] == U_last:
                new_blocks.append(b)
        if len(new_blocks) < len(blocks):
            blocks[:] = new_blocks


def _trim_flat_weights_before_first_range(category_specs):
    """
    For each category: if the first block is Flat and there are adder/range blocks after it,
    drop flat weight columns above the computed threshold (see _flat_trim_threshold_for_category_blocks).
    """
    for _cat_name, blocks in category_specs:
        if not blocks:
            continue
        first_unit, first_weights, first_label = blocks[0]
        if first_label != 'Flat':
            continue
        threshold = _flat_trim_threshold_for_category_blocks(blocks)
        if threshold is None:
            continue

        def _keep_flat(w):
            try:
                return float(w) <= threshold
            except (ValueError, TypeError):
                return True

        kept = [w for w in first_weights if _keep_flat(w)]
        if len(kept) < len(first_weights):
            blocks[0] = (first_unit, kept, first_label)


def _trim_flat_weights_union_by_service(category_specs, category_merge_meta):
    """
    For each cost category, union flat columns across services that share that category,
    trimming each service's flats only against its own first adder break, then union.
    """
    for cat_name, blocks in category_specs:
        meta = category_merge_meta.get(cat_name)
        if not meta:
            _trim_flat_weights_before_first_range([(cat_name, blocks)])
            continue
        if not blocks or blocks[0][2] != 'Flat':
            continue
        first_unit, _first_weights, row4 = blocks[0]
        per_flat = meta.get('per_service_flat') or {}
        mins = meta.get('service_adder_min') or {}
        keep = set()
        for svc, ws in per_flat.items():
            m = mins.get(svc)
            for w in ws:
                if m is None:
                    keep.add(w)
                else:
                    try:
                        if float(w) <= m:
                            keep.add(w)
                    except (ValueError, TypeError):
                        keep.add(w)
        new_flat = sorted(keep, key=_weight_sort_key)
        blocks[0] = (first_unit, new_flat, row4)


def _scan_category_merge_meta(main_costs):
    """
    First pass: per formatted cost_category, collect flat weights per service and ordered
    adder blocks (p/X unit + ranges) per service. service_adder_min is then set to the
    max flat weight to keep per service — either the legacy rule (min range start across
    adders when all units match) or the mixed-unit rule (see _threshold_from_ordered_adder_blocks).
    Used to split one PDF label into multiple Excel cost groups when breakpoints differ.
    """
    category_merge_meta = {}
    seen_adder = set()
    last_base = None

    def _empty_mmeta():
        return {
            'per_service_flat': defaultdict(set),
            'service_adder_min': {},
            'service_adder_blocks': defaultdict(list),
        }

    for rate_card in main_costs:
        service_type = (rate_card.get('service_type') or '').strip()
        cost_category_raw = rate_card.get('cost_category') or ''
        pricing = rate_card.get('pricing', [])

        if _is_adder_section(rate_card):
            if last_base is None:
                continue
            unit = _parse_adder_unit(cost_category_raw)
            rate_by = f"p/{unit} unit"
            weights_adder = []
            for pe in pricing:
                w = pe.get('weight', '')
                if w:
                    weights_adder.append(_normalize_adder_weight(w))
            weights_sorted = sorted(weights_adder, key=_adder_range_sort_key)
            # Include service_type: EXPORT/IMPORT/THIRD often share identical adder *shapes*
            # on the same base category; deduping only (base, rate_by, weights) would skip
            # later services and leave service_adder_min unset (wrong "no adder" variant).
            sig = (last_base, service_type, rate_by, tuple(weights_sorted))
            if sig in seen_adder:
                continue
            seen_adder.add(sig)

            mmeta = category_merge_meta.setdefault(last_base, _empty_mmeta())
            mmeta['service_adder_blocks'][service_type].append((rate_by, tuple(weights_sorted)))
            continue

        base = _format_cost_category(cost_category_raw)
        last_base = base
        weights_set = set()
        for pe in pricing:
            w = pe.get('weight', '')
            if w:
                weights_set.add(w)
        weights_sorted = sorted(weights_set, key=_weight_sort_key)

        mmeta = category_merge_meta.setdefault(base, _empty_mmeta())
        for w in weights_sorted:
            mmeta['per_service_flat'][service_type].add(w)

    for _base, mmeta in category_merge_meta.items():
        sb = mmeta.get('service_adder_blocks') or {}
        new_mins = {}
        for svc, ordered in sb.items():
            t = _threshold_from_ordered_adder_blocks([(rb, list(ws)) for rb, ws in ordered])
            if t is not None:
                new_mins[svc] = t
        mmeta['service_adder_min'] = new_mins

    return category_merge_meta


def _variant_suffix_for_adder_break(m, peer_breaks):
    """
    Human-readable suffix when splitting one cost_category into variants.
    peer_breaks: all distinct adder minima (and None) for services on this category.
    """
    if m is None:
        return ' — no adder ranges'
    numeric = sorted({x for x in peer_breaks if x is not None})
    same_int = [x for x in numeric if int(x) == int(m)]
    if len(set(same_int)) > 1:
        return f' — adder from {m:g} kg'
    return f' — flat to {int(m)} kg'


def _build_service_variant_maps(category_merge_meta):
    """
    For each base cost category, if services disagree on where adders start, assign
    distinct Excel header names (variants). Returns:
      base_to_service_variant: base -> {service_type -> variant_category_name}
      variant_merge_meta: variant_name -> {per_service_flat, service_adder_min} subset
    """
    base_to_service_variant = {}
    variant_merge_meta = {}

    for base, meta in category_merge_meta.items():
        per_flat = meta.get('per_service_flat') or {}
        mins = meta.get('service_adder_min') or {}
        services = set(per_flat.keys())
        if not services:
            continue

        peer_breaks = list({mins.get(svc) for svc in services})
        split = len(peer_breaks) > 1

        svc_to_variant = {}
        for svc in services:
            m = mins.get(svc)
            if not split:
                vname = base
            else:
                vname = base + _variant_suffix_for_adder_break(m, peer_breaks)
            svc_to_variant[svc] = vname

        base_to_service_variant[base] = svc_to_variant

        by_variant = defaultdict(set)
        for svc, vname in svc_to_variant.items():
            by_variant[vname].add(svc)

        for vname, svcs in by_variant.items():
            variant_merge_meta[vname] = {
                'per_service_flat': {s: set(per_flat[s]) for s in svcs if s in per_flat},
                'service_adder_min': {s: mins[s] for s in svcs if s in mins},
            }

    return base_to_service_variant, variant_merge_meta


# MainCosts – matrix (lane) view builder
# ---------------------------------------------------------------------------

def build_matrix_main_costs(main_costs, metadata, zoning_matrix=None, country_zoning=None):
    """
    Build the main pricing table (called the "Matrix view") for the MainCosts Excel tab.

    WHAT THE OUTPUT LOOKS LIKE:
    Each output row = one "lane" = one unique combination of service type + zone.
    All cost categories (Envelope, Documents, Parcels …) for the same lane are
    combined into a single row, with prices stored as separate columns per weight.

    Example output row:
        Lane# | Origin | Destination | Service              | Matrix zone | Envelope 0.5KG | Envelope 1KG | Documents 0.5KG …
        1     | France | Zone 1      | DHL EXPRESS EXPORT   |             | 12.50          | 15.00        | 10.00 …

    HOW MATRIX ZONES ARE DETECTED:
    A zone is flagged as a "Matrix zone" (needs expansion via ZoningMatrix) using
    _zone_needs_matrix_lookup(), which applies a two-step rule:
      1. If a ZoningMatrix whose name matches this service exists → ALL zones for
         that service are matrix zones (regardless of whether they contain letters).
      2. If no matching matrix is found → only flag the zone if its identifier is
         exactly one letter (e.g. "A", "B") as a last-resort fallback.

    Parameters:
      main_costs     – list of rate card sections from the extracted JSON
      metadata       – metadata dict (client, carrier, validity_date …)
      zoning_matrix  – raw ZoningMatrix rows (optional; used to pre-build the lookup
                       so matrix-zone detection is accurate before expansion runs)
      country_zoning – raw CountryZoning rows (optional). When single-zone DHL EXPRESS
                       DOMESTIC applies and a **DHL EXPRESS DOMESTIC ZONING** block exists
                       here (not THIRD COUNTRY only), Origin/Destination use the short
                       rate label (e.g. ``DOMESTIC_ZONE_A``) instead of the carrier country.

    Returns two things:
      rows           – the list of lane rows described above
      category_specs – a description of each cost-category column group,
                       used by write_matrix_sheet() to draw the header.
                       If services share one PDF cost label but use different first-adder
                       breakpoints, names are split into variants, e.g.
                       "… — flat to 10 kg" vs "… — flat to 30 kg" vs "… — no adder ranges".
    """
    # Build the zoning lookup once up front so _zone_needs_matrix_lookup can use it
    # during PASS 2 to decide which zones need matrix expansion.
    # If no zoning_matrix was passed in, the lookup will be empty and the fallback
    # single-letter rule will apply instead.
    zoning_lookup = parse_zoning_matrix(zoning_matrix) if zoning_matrix else {}

    # Scan: flat weights + adder breakpoints per (base category, service) so we can
    # split one PDF label into multiple Excel cost groups when breakpoints differ.
    base_merge_meta = _scan_category_merge_meta(main_costs)
    base_to_service_variant, variant_merge_meta = _build_service_variant_maps(base_merge_meta)

    # =======================================================================
    # PASS 1 – Figure out what columns the header needs.
    # category_specs: list of (cost_cat_name, blocks) where
    #   blocks = [(weight_unit, weights_list, row4_label), ...]
    # cost_cat_name is a *variant* when services disagree on adder breaks (e.g. flat to 10 vs 30).
    # Normal sections have one block with row4_label "Flat".
    # Adder sections are merged into the previous category as an extra block
    # with weight_unit and row4_label like "p/0.5 unit", weights like "10.1-20".
    # =======================================================================
    category_specs = []   # (cost_cat_name, [(weight_unit, weights, row4_label), ...])
    seen_categories = {}  # variant cost_cat_name -> index in category_specs
    seen_adder_per_category = set()
    last_category_idx = -1
    _debug_main_costs = False

    for rate_card in main_costs:
        cost_category_raw = rate_card.get('cost_category') or ''
        service_type = (rate_card.get('service_type') or '').strip()
        pricing = rate_card.get('pricing', [])

        if _is_adder_section(rate_card):
            if not category_specs or last_category_idx < 0:
                if _debug_main_costs:
                    print(f"[DEBUG MainCosts] ADDER skipped (no category yet): service={service_type!r} cost={cost_category_raw!r}")
                continue
            prev_name = category_specs[last_category_idx][0]
            unit = _parse_adder_unit(cost_category_raw)
            rate_by = f"p/{unit} unit"
            weights_adder = []
            for pe in pricing:
                w = pe.get('weight', '')
                if w:
                    weights_adder.append(_normalize_adder_weight(w))
            weights_adder_sorted = sorted(weights_adder, key=_adder_range_sort_key)
            sig = (prev_name, rate_by, tuple(weights_adder_sorted))
            if sig in seen_adder_per_category:
                if _debug_main_costs:
                    print(f"[DEBUG MainCosts] ADDER skipped (duplicate): service={service_type!r} attach_to={prev_name!r} rate_by={rate_by!r} weights={weights_adder_sorted}")
                continue
            seen_adder_per_category.add(sig)
            prev_blocks = category_specs[last_category_idx][1]
            prev_blocks.append((rate_by, weights_adder_sorted, rate_by))
            if _debug_main_costs:
                print(f"[DEBUG MainCosts] ADDER attached: service={service_type!r} cost_raw={cost_category_raw!r} -> ATTACH_TO( last processed category )={prev_name!r} rate_by={rate_by!r} weights={weights_adder_sorted}")
            continue

        # Normal section
        base_category = _format_cost_category(cost_category_raw)
        svc_map = base_to_service_variant.get(base_category) or {}
        cost_category = svc_map.get(service_type, base_category)
        weight_unit = rate_card.get('weight_unit') or 'KG'
        weights_set = set()
        for pe in pricing:
            w = pe.get('weight', '')
            if w:
                weights_set.add(w)
        weights_sorted = sorted(weights_set, key=_weight_sort_key)
        block = (weight_unit, weights_sorted, 'Flat')

        if cost_category not in seen_categories:
            seen_categories[cost_category] = len(category_specs)
            category_specs.append((cost_category, [block]))
            last_category_idx = len(category_specs) - 1
            if _debug_main_costs:
                print(f"[DEBUG MainCosts] NEW category (now last): service={service_type!r} cost_raw={cost_category_raw!r} -> category={cost_category!r} (flat weights count={len(weights_sorted)})")
        else:
            idx = seen_categories[cost_category]
            _, blocks = category_specs[idx]
            existing_unit, existing_weights, row4 = blocks[0]
            merged = set(existing_weights) | set(weights_sorted)
            merged_sorted = sorted(merged, key=_weight_sort_key)
            blocks[0] = (existing_unit, merged_sorted, row4)
            last_category_idx = idx
            if _debug_main_costs:
                print(f"[DEBUG MainCosts] MERGE into existing: service={service_type!r} cost_raw={cost_category_raw!r} -> category={cost_category!r} (last_category_idx now {last_category_idx} = this category)")

    if _debug_main_costs:
        print("[DEBUG MainCosts] --- PASS 1 summary: categories and their blocks (order = column order in Excel) ---")
        for i, (cat_name, blocks) in enumerate(category_specs):
            block_labels = []
            for b in blocks:
                unit, weights, row4 = b
                if row4 == 'Flat':
                    block_labels.append(f"Flat({len(weights)} weights)")
                else:
                    block_labels.append(f"{row4}({weights})")
            print(f"  [{i}] {cat_name!r} -> blocks: {block_labels}")

    for _cat_name, blocks in category_specs:
        blocks.sort(key=_adder_block_sort_key)

    _trim_flat_weights_union_by_service(category_specs, variant_merge_meta)
    _drop_intermediate_adder_blocks(category_specs)

    domestic_single_zone = _dhl_express_domestic_single_cost_zone_only(main_costs)
    cz = country_zoning or []
    has_domestic_zoning_rate = _country_zoning_has_dhl_express_domestic_zoning(cz)
    use_domestic_rate_labels = bool(
        domestic_single_zone and has_domestic_zoning_rate and cz
    )
    zone_label_lookup_domestic = (
        build_zone_label_lookup(cz) if use_domestic_rate_labels else {}
    )

    # =======================================================================
    # PASS 2 – Build one row per lane (service + zone combination).
    # =======================================================================
    lane_rows = {}   # (service_type, zone_name) -> row dict
    prev_cost_category = None   # variant name for merging adder sections

    for rate_card in main_costs:
        service_type = (rate_card.get('service_type') or '').strip()
        cost_category_raw = rate_card.get('cost_category') or ''
        zone_headers = rate_card.get('zone_headers', {})
        pricing = rate_card.get('pricing', [])

        if _is_adder_section(rate_card):
            if prev_cost_category is None:
                continue
            cost_category = prev_cost_category
            _key_weight = lambda w: _normalize_adder_weight(w)
        else:
            base_category = _format_cost_category(cost_category_raw)
            svc_map = base_to_service_variant.get(base_category) or {}
            cost_category = svc_map.get(service_type, base_category)
            prev_cost_category = cost_category
            _key_weight = lambda w: w

        service_lower = service_type.lower()
        is_import = 'import' in service_lower
        is_export = 'export' in service_lower

        # Reorganise the pricing list from weight-first to zone-first
        zone_price_matrix = {}
        for price_entry in pricing:
            weight = price_entry.get('weight', '')
            zone_prices = price_entry.get('zone_prices', {})
            for zone_key, price in zone_prices.items():
                zone_name = zone_headers.get(zone_key, zone_key)
                if zone_name not in zone_price_matrix:
                    zone_price_matrix[zone_name] = {}
                zone_price_matrix[zone_name][weight] = price

        for zone_name, weight_prices in zone_price_matrix.items():
            key = (service_type, zone_name)

            if key not in lane_rows:
                origin = zone_name if is_import else ''
                destination = zone_name if is_export else ''
                # Single-zone DHL EXPRESS DOMESTIC: one lane, no matrix expansion — O/D filled with carrier country below.
                if domestic_single_zone and (service_type or '').strip().upper() == 'DHL EXPRESS DOMESTIC':
                    needs_lookup = False
                    matrix_zone = ''
                else:
                    # Use the two-step rule: service-matrix match first, single-letter fallback second
                    needs_lookup = _zone_needs_matrix_lookup(zone_name, service_type, zoning_lookup)
                    matrix_zone = zone_name if needs_lookup else ''
                lane_rows[key] = {
                    'Origin': origin,
                    'Destination': destination,
                    'Service': service_type,
                    'Matrix zone': matrix_zone,
                }

            row = lane_rows[key]
            for weight, price in weight_prices.items():
                row[(cost_category, _key_weight(weight))] = price

    # Get the carrier's country name (e.g. "Netherlands") — used to fill Origin/Destination
    # for domestic and non-zoned lanes where the carrier country is the implicit value.
    carrier_last = global_country(metadata)

    # Sort the lanes: first by service name (alphabetical), then by zone (numeric before letter)
    sorted_keys = sorted(lane_rows.keys(), key=lambda k: (k[0], _zone_sort_key(k[1])))

    rows = []
    for lane, key in enumerate(sorted_keys, 1):
        row = lane_rows[key].copy()
        row['Lane #'] = lane

        service = (row.get('Service') or '').strip()
        matrix_zone = (row.get('Matrix zone') or '').strip()

        if service.upper() == 'DHL EXPRESS DOMESTIC':
            # Single-zone domestic + CountryZoning "DHL EXPRESS DOMESTIC ZONING" → short
            # label (e.g. DOMESTIC_ZONE_A); otherwise carrier country for both sides.
            zone_nm = key[1] if isinstance(key, tuple) and len(key) > 1 else ''
            dom_label = _domestic_zone_short_label(zone_label_lookup_domestic, zone_nm)
            if dom_label:
                row['Origin'] = dom_label
                row['Destination'] = dom_label
            elif carrier_last:
                row['Origin'] = carrier_last
                row['Destination'] = carrier_last
        elif not matrix_zone:
            # Non-zoned lane: fill whichever side is still empty with the carrier country
            if carrier_last:
                if not (row.get('Origin') or '').strip():
                    row['Origin'] = carrier_last
                if not (row.get('Destination') or '').strip():
                    row['Destination'] = carrier_last

        rows.append(row)

    return rows, category_specs


def apply_zone_labels_to_main_costs(matrix_rows, zone_label_lookup):
    """
    Replace raw zone names in Origin/Destination with meaningful short labels.

    PURPOSE:
    After build_matrix_main_costs() runs, zoned lanes have Origin or Destination
    values like "Zone 8".  This function replaces those with a label that includes
    the service context, e.g. "ECONOMY_EXP_ZONE_8", so the analyst can immediately
    see which zoning scheme the zone belongs to.

    HOW IT WORKS:
    For each lane row:
      1. Check if Origin or Destination looks like a zone (starts with "Zone ").
      2. Extract the zone number (e.g. "Zone 8" -> "8").
      3. Convert the Service name to its short prefix using the same
         _transform_rate_name_to_short() logic used to build the lookup.
      4. Look up (short_prefix, zone_number) in the zone_label_lookup dict.
      5. If found, replace the Origin/Destination value with the label.

    Rows where Origin/Destination is a country name (not a zone) are left unchanged.

    Parameters:
      matrix_rows       – list of lane row dicts from build_matrix_main_costs()
      zone_label_lookup – dict built by build_zone_label_lookup() in transform_other_tabs.py
                          keys: (short_prefix, zone_number), values: label string

    Returns the same list of rows with Origin/Destination values updated in place.
    """
    if not zone_label_lookup or not matrix_rows:
        return matrix_rows

    # Import here to avoid circular imports (transform_other_tabs imports nothing from here)
    from transform_other_tabs import _transform_rate_name_to_short

    _zone_re = re.compile(r'(?i)^zone\s+(.+)$')

    for row in matrix_rows:
        service = (row.get('Service') or '').strip()
        short_prefix = _transform_rate_name_to_short(service)
        if not short_prefix:
            continue

        for field in ('Origin', 'Destination'):
            val = (row.get(field) or '').strip()
            m = _zone_re.match(val)
            if not m:
                continue   # not a zone value — leave unchanged

            zone_number = m.group(1).strip()
            label = zone_label_lookup.get((short_prefix, zone_number))
            if label:
                row[field] = label

    return matrix_rows


def _origin_layout_sort_tuple(origin):
    """
    Derive a sortable tuple from Origin for matrix-expanded rows.
    Prefer trailing ``_N`` (e.g. ``WW_EXP_IMP_ZONE_1``), then ``Zone N``, then ``Zone L``.
    """
    s = (origin or '').strip()
    if not s:
        return (2, 0, '')
    m = re.search(r'_(\d+)$', s)
    if m:
        return (0, int(m.group(1)), '')
    m = re.search(r'(?i)zone\s+(\d+)', s)
    if m:
        return (0, int(m.group(1)), '')
    m = re.search(r'(?i)zone\s+([A-Z])\b', s)
    if m:
        return (1, ord(m.group(1).upper()), '')
    return (2, 0, s.upper())


def sort_main_costs_rows_for_layout(matrix_rows):
    """
    Final row order for the MainCosts sheet (does not change pricing or zone logic).

    Rows with a non-empty **Matrix zone** are grouped by **Service**, then sorted by
    **Origin** so numeric zone indices come in order (e.g. ``..._ZONE_1`` before
    ``..._ZONE_2``, and ``Zone 1`` before ``Zone 10``). Letter zones (``Zone A``)
    sort after numeric-style origins. Rows **without** a Matrix zone keep their
    relative order (stable).

    **Lane #** is reassigned 1..n after sorting.
    """
    if not matrix_rows:
        return matrix_rows

    enumerated = list(enumerate(matrix_rows))

    def row_sort_key(entry):
        orig_idx, row = entry
        svc = (row.get('Service') or '').strip()
        mz = (row.get('Matrix zone') or '').strip()
        if not mz:
            return (svc, 1, orig_idx)
        ot = _origin_layout_sort_tuple(row.get('Origin') or '')
        dest = (row.get('Destination') or '').strip()
        return (svc, 0, ot, dest, orig_idx)

    enumerated.sort(key=row_sort_key)
    out = [row for _, row in enumerated]
    for lane, row in enumerate(out, 1):
        row['Lane #'] = lane
    return out


def expand_main_costs_lanes_by_zoning(matrix_rows, zoning_matrix):
    """
    Replace abstract letter-zone rows with real Origin/Destination rows.

    PROBLEM THIS SOLVES:
    After build_matrix_main_costs() runs, some lanes have a "Matrix zone" value
    like "Zone A" instead of real origin/destination countries.  "Zone A" is just
    a code that means "all the origin/destination pairs that belong to group A".
    This function looks up those pairs and creates one concrete row per pair.

    EXAMPLE:
    Before expansion:
        Lane | Origin | Destination | Service          | Matrix zone | Price
        1    |        |             | DHL EXPRESS WW   | Zone A      | 12.50

    After expansion (if Zone A covers origin 1->dest 3 and origin 2->dest 3):
        Lane | Origin | Destination | Service          | Matrix zone | Price
        1    | Zone 1 | Zone 3      | DHL EXPRESS WW   | Zone A      | 12.50
        2    | Zone 2 | Zone 3      | DHL EXPRESS WW   | Zone A      | 12.50

    Rows that already have numeric zones (no Matrix zone value) are left unchanged.
    After all expansion is done, Lane numbers are reassigned from 1 upward.
    """
    if not matrix_rows:
        return matrix_rows

    # Build the full (matrix_name, zone_letter) -> [(origin, dest), ...] lookup
    zoning_lookup = parse_zoning_matrix(zoning_matrix)
    if not zoning_lookup:
        print("[DEBUG] expand_matrix_zones: zoning_lookup is empty; no expansion")
        return matrix_rows

    matrix_names_in_lookup = sorted({k[0] for k in zoning_lookup})
    print(f"[DEBUG] expand_matrix_zones: lookup has {len(zoning_lookup)} keys; matrix names: {matrix_names_in_lookup}")

    expanded = []
    debug_logged = set()   # (reason, service_snippet) to avoid repeating same message

    for row in matrix_rows:
        matrix_zone = (row.get('Matrix zone') or '').strip()
        service = (row.get('Service') or '').strip()

        if not matrix_zone:
            expanded.append(row)
            continue

        zone_letter = _matrix_zone_to_letter(matrix_zone)
        if not zone_letter:
            key = ("zone_letter_empty", service[:50])
            if key not in debug_logged:
                debug_logged.add(key)
                print(f"[DEBUG] expand_matrix_zones: SKIP zone_letter empty  service={service!r}  matrix_zone={matrix_zone!r} -> letter={zone_letter!r}")
            expanded.append(row)
            continue

        matrix_name = _find_matrix_for_service(zoning_lookup, service)
        if not matrix_name:
            key = ("no_matrix_name", service[:50])
            if key not in debug_logged:
                debug_logged.add(key)
                print(f"[DEBUG] expand_matrix_zones: SKIP no matrix for service  service={service!r}  matrix_zone={matrix_zone!r}  zone_letter={zone_letter!r}")
            expanded.append(row)
            continue

        key = (matrix_name, zone_letter)
        pairs = zoning_lookup.get(key, [])
        if not pairs:
            key_dbg = ("no_pairs", matrix_name, zone_letter)
            if key_dbg not in debug_logged:
                debug_logged.add(key_dbg)
                available_letters = sorted({k[1] for k in zoning_lookup if k[0] == matrix_name})
                print(f"[DEBUG] expand_matrix_zones: SKIP no pairs  service={service!r}  matrix_name={matrix_name!r}  zone_letter={zone_letter!r}  available_letters_for_this_matrix={available_letters}")
            expanded.append(row)
            continue

        key_ok = ("expanded", matrix_name, zone_letter)
        if key_ok not in debug_logged:
            debug_logged.add(key_ok)
            print(f"[DEBUG] expand_matrix_zones: OK  service={service[:45]!r}  matrix_name={matrix_name!r}  zone_letter={zone_letter!r}  -> {len(pairs)} pair(s)")

        # Create one copy of the row per (origin, destination) pair
        for origin_zone, dest_zone in pairs:
            new_row = row.copy()
            new_row['Origin'] = f"Zone {origin_zone}" if origin_zone else ''
            new_row['Destination'] = f"Zone {dest_zone}" if dest_zone else ''
            expanded.append(new_row)

    # Reassign Lane # sequentially after expansion
    for lane, row in enumerate(expanded, 1):
        row['Lane #'] = lane

    return expanded
