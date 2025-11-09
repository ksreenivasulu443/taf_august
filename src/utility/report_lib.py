import datetime
import os


project_path =  os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
report_dir = os.path.join(project_path,'report')
os.makedirs(report_dir, exist_ok=True)
#
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")

report_filename = os.path.join(report_dir, f"report_{timestamp}.txt")
print(report_filename)


def write_output(validation_type, status, details):
    with open(report_filename, "a") as report:
        report.write(f"{validation_type}: {status} Details: {details}\n ")









