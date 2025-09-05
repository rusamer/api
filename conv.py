#!/usr/bin/env python3
import hashlib

infile = "data/rockyou.txt"             # input wordlist
outfile = "data/rockyou_pwned.txt"      # output SHA1 hashes

print("⏳ Converting rockyou.txt → SHA1 hashes (streaming mode)...")

count = 0
with open(infile, "r", encoding="latin-1", errors="ignore") as f_in, open(outfile, "w") as f_out:
    for i, line in enumerate(f_in, 1):
        pw = line.strip()
        if not pw:
            continue

        # compute SHA1 in uppercase
        sha1 = hashlib.sha1(pw.encode("utf-8", errors="ignore")).hexdigest().upper()

        # write hash + fake count = 1
        f_out.write(f"{sha1}:1\n")
        count += 1

        # progress log every 100k lines
        if i % 100000 == 0:
            print(f"   ... processed {i:,} lines")

print(f"🎉 Done! Written {count:,} hashes to {outfile}")
