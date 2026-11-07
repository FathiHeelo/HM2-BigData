"""Part 2: find students enrolled in a course, sorted by last name, and check the query plan."""
import argparse
import os

from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")


def ask_course_id():
    while True:
        text = input("Enter course ID: ")
        try:
            return int(text)
        except ValueError:
            print("Course ID must be an integer.")


def find_stages(plan, stage_name):
    """Collect every stage with the given name from a (nested) explain plan."""
    found = []
    if isinstance(plan, dict):
        if plan.get("stage") == stage_name:
            found.append(plan)
        for value in plan.values():
            found.extend(find_stages(value, stage_name))
    elif isinstance(plan, list):
        for item in plan:
            found.extend(find_stages(item, stage_name))
    return found


parser = argparse.ArgumentParser(description="Query students by course ID.")
parser.add_argument("--course", type=int, help="course ID (prompted if omitted)")
args = parser.parse_args()

course_id = args.course if args.course is not None else ask_course_id()

client = MongoClient(MONGO_URI)
students = client["university"]["students"]

query = {"courseIds": course_id}
sort_order = [("lastName", 1), ("firstName", 1)]
print("Query:", query, "sort:", sort_order)

results = list(students.find(query).sort(sort_order))
for s in results:
    print(s["_id"], s["firstName"] + " " + s["lastName"], s["gpa"])
print("Matches:", len(results))

plan = students.find(query).sort(sort_order).explain()
winning_plan = plan["queryPlanner"]["winningPlan"]
print("Winning plan:", winning_plan)

ixscans = find_stages(winning_plan, "IXSCAN")
index_names = [s.get("indexName") for s in ixscans]
print("IXSCAN used?", bool(ixscans), "indexes:", index_names)
print("IXSCAN on courseIds_1?", "courseIds_1" in index_names)
