"""Part 1: load students.csv into MongoDB (university.students) and write students.json."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")


def parse_line(line):
    # CourseIDs is unquoted and contains commas, e.g. 1,Alice,Johnson,1998-03-15,[101,102,103],3.8
    # so take the first 4 fields and the last one; everything in between is the course list.
    parts = line.strip().split(",")

    course_text = ",".join(parts[4:-1])
    course_ids = [int(c) for c in course_text.strip("[]").split(",")]

    return {
        "_id": int(parts[0]),
        "firstName": parts[1],
        "lastName": parts[2],
        "dateOfBirth": datetime.strptime(parts[3], "%Y-%m-%d").replace(tzinfo=timezone.utc),
        "courseIds": course_ids,
        "gpa": float(parts[-1]),
    }


docs = []
with open(DATA_DIR / "students.csv", encoding="utf-8-sig") as f:
    next(f)  # header
    for line in f:
        if line.strip():
            docs.append(parse_line(line))

print("Parsed:", len(docs))
print(docs[0])

# One JSON document per line; dateOfBirth as an ISO string
with open(DATA_DIR / "students.json", "w", encoding="utf-8") as f:
    for d in docs:
        row = dict(d)
        row["dateOfBirth"] = d["dateOfBirth"].strftime("%Y-%m-%dT%H:%M:%SZ")
        f.write(json.dumps(row) + "\n")
print("Saved students.json")

client = MongoClient(MONGO_URI)
db = client["university"]
db.drop_collection("students")  # makes the script re-runnable
students = db["students"]

result = students.insert_many(docs)
print("Inserted:", len(result.inserted_ids))

students.create_index("courseIds")  # multikey (array field)
students.create_index("gpa")

print("Indexes:")
for idx in students.list_indexes():
    print("  ", dict(idx))
