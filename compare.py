def parse_symbols(s: str) -> set:
    return {x.strip() for x in s.split(",") if x.strip()}


print("Paste first list (comma-separated symbols), then Enter:")
line1 = input().strip()

print("Paste second list (comma-separated symbols), then Enter:")
line2 = input().strip()

set1 = parse_symbols(line1)
set2 = parse_symbols(line2)

only_in_1 = sorted(set1 - set2)
only_in_2 = sorted(set2 - set1)

print()
print("Present in first list only (not in second):")
print(only_in_1)
print()
print("Present in second list only (not in first):")
print(only_in_2)
