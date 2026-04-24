from pathlib import Path

output_path = Path("outputs/dns_decision_summary.txt")

lines = [
    "DNS Decision Summary",
    "=" * 40,
    "",
    "Initial exploration results:",
    "- The dns.txt file was successfully read.",
    "- The explored sample showed 3 columns only.",
    "- No missing values were observed in the tested sample.",
    "- The first column appears temporal, while the other two columns appear to be computer-like identifiers.",
    "",
    "Redteam-aligned sample findings:",
    "- A DNS sample was extracted near the beginning of redteam activity.",
    "- The selected dns_redteam_sample covered timestamps 150881 -> 167943.",
    "- Almost all rows in the sample fell inside the redteam time range.",
    "- Computer overlap with redteam was observed.",
    "- However, no direct src/dst computer pair matches were found against the redteam file.",
    "",
    "Decision:",
    "The DNS file will not be adopted as a primary or strong secondary input source in the current prototype stage.",
    "",
    "Reasoning:",
    "- Authentication data remains the most semantically rich and analytically useful source for the current project goal.",
    "- Process data showed a stronger exploratory signal than DNS when evaluated with redteam-aligned samples.",
    "- Although DNS overlaps temporally with redteam and shares some computers, it did not provide direct pair-level evidence in the explored sample.",
    "- Therefore, DNS is considered weaker than both auth and proc for the current prototype scope.",
    "",
    "Conclusion:",
    "In the current phase, dns is treated as an explored but non-adopted source. It remains a possible future extension, but not a component of the implemented prototype.",
]

text = "\n".join(lines)
output_path.write_text(text, encoding="utf-8")

print(text)
print("\nSaved to:", output_path)