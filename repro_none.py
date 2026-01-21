
event = {'segment_id': None, 'segment_to_run': None}
val = event.get('segment_id') or event.get('segment_to_run', 0)
print(f"Result with explicit None: {val}, Type: {type(val)}")

event2 = {'segment_id': 0}
val2 = event2.get('segment_id') or event2.get('segment_to_run', 0)
print(f"Result with 0: {val2} (Buggy? Should be 0 but 'or' might make it check next)")

event3 = {'segment_id': 0, 'segment_to_run': 1}
val3 = event3.get('segment_id') or event3.get('segment_to_run', 0)
print(f"Result with 0 and 1: {val3} (Should start with 0!)")
