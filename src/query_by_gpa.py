"""Part 3: find students with GPA strictly greater than a value, optionally limited to one course."""
import argparse
import os

from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")


def parse_gpa(text):
    try:
        value = float(text)
    except ValueError:
        raise ValueError("GPA must be a number.") from None
    if not 0.0 <= value <= 4.0:
        raise ValueError("GPA must be between 0.0 and 4.0.")
    return value


def gpa_arg(text):
    try:
        return parse_gpa(text)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None


def ask_course_id():
    while True:
        text = input("Course ID (Enter to skip): ").strip()
        if text == "":
            return None
        try:
            return int(text)
        except ValueError:
            print("Course ID must be an integer.")


def ask_min_gpa():
    while True:
        text = input("GPA greater than (Enter to skip): ").strip()
        if text == "":
            return None
        try:
            return parse_gpa(text)
        except ValueError as e:
            print(e)


parser = argparse.ArgumentParser(description="Query students by GPA (and optionally course).")
parser.add_argument("--gpa", type=gpa_arg, help="return students with GPA > this value (0.0-4.0)")
parser.add_argument("--course", type=int, help="also require this course ID")
args = parser.parse_args()

# Any argument given -> non-interactive; otherwise prompt as before
if args.gpa is not None or args.course is not None:
    course_id, min_gpa = args.course, args.gpa
else:
    course_id = ask_course_id()
    min_gpa = ask_min_gpa()

if course_id is None and min_gpa is None:
    print("Please enter at least one condition.")
    raise SystemExit

query = {}
if course_id is not None:
    query["courseIds"] = course_id
if min_gpa is not None:
    query["gpa"] = {"$gt": min_gpa}

print("Query:", query)

client = MongoClient(MONGO_URI)
students = client["university"]["students"]

results = list(students.find(query).sort([("gpa", -1), ("lastName", 1)]))
for s in results:
    print(s["_id"], s["firstName"] + " " + s["lastName"], s["gpa"])
print("Matches:", len(results))
