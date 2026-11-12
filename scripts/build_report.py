"""Build the assignment PDF from source screenshots and genuine saved evidence."""

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak,
)

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "reports" / "evidence"
DESTINATION = ROOT / "reports" / "airflow_etl_report.pdf"
INK = colors.HexColor("#10253b")
TEAL = colors.HexColor("#087e8b")
styles = getSampleStyleSheet()
styles.add(ParagraphStyle("ReportTitle", fontSize=29, leading=34, textColor=INK, spaceAfter=16))
styles.add(ParagraphStyle("SectionTitle", fontSize=19, leading=24, textColor=INK, spaceAfter=12))
styles.add(ParagraphStyle("ReportBody", fontSize=10.5, leading=15, textColor=INK, spaceAfter=9))
styles.add(ParagraphStyle("Caption", fontSize=9, leading=12, textColor=colors.HexColor("#4b6176"), spaceBefore=8, spaceAfter=12))
styles.add(ParagraphStyle("Kicker", fontSize=10, leading=14, textColor=TEAL, spaceAfter=10))
story = []


def paragraph(text, style="ReportBody"):
    story.append(Paragraph(text, styles[style]))


def page(title, kicker=None):
    if story:
        story.append(PageBreak())
    if kicker:
        paragraph(kicker.upper(), "Kicker")
    paragraph(title, "SectionTitle")


def figure(name, caption, max_height=570):
    path = EVIDENCE / f"{name}.png"
    if not path.exists():
        paragraph(f"Evidence unavailable: {caption}")
        return
    width, height = ImageReader(str(path)).getSize()
    factor = min(499 / width, max_height / height)
    story.append(Image(str(path), width=width * factor, height=height * factor))
    paragraph(caption, "Caption")


def footer(canvas, document):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#d5dfe9"))
    canvas.line(48, 43, A4[0] - 48, 43)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#4b6176"))
    canvas.drawString(48, 29, "AIRFLOW ETL  /  FRANKFURTER TO LOCAL MONGODB")
    canvas.drawRightString(A4[0] - 48, 29, str(document.page))
    canvas.restoreState()


def main():
    date = datetime.now(ZoneInfo("Asia/Karachi")).strftime("%d %B %Y")
    paragraph("MLOPS ASSIGNMENT  /  IMPLEMENTATION REPORT", "Kicker")
    paragraph("Daily Currency<br/>Exchange ETL", "ReportTitle")
    paragraph("Apache Airflow with Astro CLI, Frankfurter v2, MongoDB and Mongo Express")
    paragraph(f"Prepared for: <b>saadzafar0</b><br/>Report generated: {date}<br/>Repository: <link href='https://github.com/saadzafar0/airflow-etl' color='#087e8b'>github.com/saadzafar0/airflow-etl</link>")
    story.append(Spacer(1, 14))
    paragraph("Objective", "SectionTitle")
    paragraph("Implement a daily three-task pipeline that extracts exchange rates, validates and transforms the response, and loads six-field records into a MongoDB database running locally in Docker. Named Airflow XCom keys transfer data between tasks.")
    rows = [["Requirement", "Implementation"],
        ["Astro CLI / Airflow", "Astro project and pinned Runtime Dockerfile"],
        ["Exactly three tasks", "Extract > Transform > Load"],
        ["Named XCom transfer", "raw_exchange_rates; transformed_exchange_rates"],
        ["Local database and admin UI", "MongoDB + Mongo Express Docker services"],
        ["Networking / persistence", "Internal bridge; named data/config volumes"],
        ["Repeatable loading", "Unique pair/date index and MongoDB upserts"],
        ["Submission", "Source, evidence screenshots, PDF, incremental Git commits"]]
    table = Table([[Paragraph(cell, styles["ReportBody"]) for cell in row] for row in rows], colWidths=[180, 319])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e7f2f4")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LINEBELOW", (0, 0), (-1, -1), 0.3, colors.HexColor("#d5dfe9")),
    ]))
    story.append(table)
    paragraph("Evidence policy: source images are browser captures of the checked-in files; output images show saved command/API results. Runtime completion is reported only when supported by actual execution evidence.", "Caption")

    page("The three ETL tasks and named XComs", "01 / orchestration")
    paragraph("Each task explicitly pushes or pulls its named XCom. Pulls identify the producing task. Automatic return-value XComs are disabled, so the implementation visibly satisfies the assignment's named-transfer requirement.")
    figure("code_dag_tasks", "Figure 1. Actual DAG task functions showing extraction, named XCom transfer, transformation and MongoDB loading.", 590)

    page("Daily schedule and task dependencies", "02 / scheduling")
    paragraph("The DAG runs at 07:15 Asia/Karachi (02:15 UTC). It has exactly three sequential PythonOperator tasks, two retries with a one-minute delay, one active run, and catchup disabled. The rate date always comes from the API response.")
    figure("code_dag_schedule", "Figure 2. DAG configuration and Extract > Transform > Load dependencies.", 580)

    page("Extract the latest published exchange rates", "03 / extraction")
    paragraph("The extractor requests EUR rates for USD, GBP and JPY from the public Frankfurter v2 endpoint. It checks HTTP status and response shape, applies explicit timeouts, and adds a timezone-aware UTC retrieval timestamp. Currency configuration is validated before sending the request.")
    figure("code_extract", "Figure 3. API endpoint, currency normalization, HTTP extraction and raw XCom key definitions.", 575)

    page("Transform and validate the data contract", "04 / transformation")
    paragraph("The transformation produces the six required fields, keeps the source publication date, and preserves the extraction timestamp. It rejects incomplete currency coverage, duplicate targets, wrong bases, invalid dates and invalid numeric rates. Validation completes before loading begins.")
    figure("code_transform", "Figure 4. Transformation logic implementing the required MongoDB document schema.", 575)

    page("Load safely into local MongoDB", "05 / persistence")
    paragraph("Loading creates a unique compound index on base currency, target currency and publication date. Upserts update an existing record or insert a new date/pair. Repeating the same run therefore preserves one document per pair/date. The task publishes a named summary with actual insert and match counts.")
    figure("code_load", "Figure 5. MongoDB connection, unique index, bulk upserts and load summary.", 510)

    page("Docker networking and persistent storage", "06 / local infrastructure")
    paragraph("The scheduler joins the private currency-db network while retaining Astro's default network for the public API. MongoDB exposes no host port. Mongo Express is available only at 127.0.0.1:8081. Named volumes persist the database and configuration directories; labels describe these choices.")
    figure("code_docker", "Figure 6. Actual Compose override: MongoDB, Mongo Express, networking, volumes and metadata labels.", 590)

    page("Actual API response and transformed records", "07 / live data")
    raw_path = EVIDENCE / "live-api.json"
    if raw_path.exists():
        raw = json.loads(raw_path.read_text())
        paragraph(f"The live request returned {len(raw['rates'])} rows. Retrieval timestamp: {raw['retrieved_at']}. The figures below contain actual saved API and transformation output, rather than the sample values in the assignment.")
    figure("output_api", "Figure 7. Live Frankfurter response and extraction timestamp.", 265)
    figure("output_transform", "Figure 8. Actual records mapped to the required six-field schema.", 340)

    page("Tests and runtime verification", "08 / validation")
    paragraph("Data tests cover configuration, missing and duplicate currencies, invalid numeric rates, invalid/future dates, missing XCom payloads, timezone-free timestamps and HTTP failures. The DAG import test uses the real Airflow parser when run inside the Runtime.")
    figure("output_tests", "Figure 9. Actual test results. A skipped DAG test indicates that the host lacks Airflow; in-runtime results are reported separately below.", 110)
    figure("output_runtime", "Figure 10. Actual Astro/Airflow runtime verification output.", 460)

    page("Persisted results and administration", "09 / database evidence")
    paragraph("The database evidence below is generated by reading MongoDB after execution. The unique compound index and stored documents demonstrate the load operation. The administration screenshot, when present, is captured from the running local Mongo Express service.")
    figure("mongo_express", "Figure 11. Running local Mongo Express showing the exchange_rates collection.", 290)
    figure("output_database", "Figure 12. Actual MongoDB verification output.", 320)

    page("Docker and incremental delivery", "10 / reproducibility")
    figure("output_docker", "Figure 13. Actual local Docker service, network and persistent-volume inspection.", 355)
    figure("output_git", "Figure 14. Incremental commits made during implementation.", 180)
    paragraph("Reproduce: copy .env.example to .env, run astro dev start, unpause currency_exchange_etl, and trigger it. Open Airflow at localhost:8080 and Mongo Express at 127.0.0.1:8081. Stop with astro dev stop to preserve database volumes.")

    page("Results and reference material", "11 / conclusion")
    verification_path = EVIDENCE / "verification.json"
    if verification_path.exists():
        verification = json.loads(verification_path.read_text())
        for item in verification.get("results", []):
            paragraph(item)
        for item in verification.get("limitations", []):
            paragraph(f"<b>Limitation:</b> {item}")
    else:
        paragraph("The implementation and saved live API results are available. Consult the runtime evidence pages for the execution status; absent evidence is not a successful run.")
    paragraph("Files to review", "SectionTitle")
    paragraph("dags/currency_exchange_etl.py: orchestration and named XComs.<br/>include/currency_etl.py: extract, validate, transform, load.<br/>docker-compose.override.yml: local database, admin UI, networks, volumes.<br/>tests/: data contract and real DAG import checks.<br/>reports/evidence/: screenshots and captured output.<br/>README.md: setup and verification instructions.")
    paragraph("References", "SectionTitle")
    for label, url in [
        ("Assignment specification", "https://github.com/saadzafar0/airflow-etl/blob/main/instructions.md"),
        ("Frankfurter API documentation", "https://frankfurter.dev/"),
        ("Apache Airflow XCom documentation", "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html"),
        ("Astro CLI installation", "https://www.astronomer.io/docs/astro/cli/install-cli"),
        ("Astro Runtime release notes", "https://www.astronomer.io/docs/runtime/runtime-release-notes"),
        ("MongoDB official Docker image", "https://hub.docker.com/_/mongo"),
    ]:
        paragraph(f"<link href='{url}' color='#087e8b'>{label}</link>")
    SimpleDocTemplate(
        str(DESTINATION), pagesize=A4, rightMargin=48, leftMargin=48,
        topMargin=45, bottomMargin=56,
        title="Daily Currency Exchange ETL - Implementation Report",
        author="saadzafar0", subject="Airflow ETL assignment with genuine code and execution screenshots",
    ).build(story, onFirstPage=footer, onLaterPages=footer)
    print(DESTINATION)


if __name__ == "__main__":
    main()
