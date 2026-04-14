from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.financial_service import FinancialService
from app.utils.auth import get_current_tenant

import matplotlib.pyplot as plt
import csv
import uuid

router = APIRouter()

# =========================
# 📊 DOWNLOAD CHART (PNG)
# =========================
@router.get("/chart")
def download_chart(
    db: Session = Depends(get_db),
    tenant=Depends(get_current_tenant)
):
    data = FinancialService.get_charts(db, tenant)["country_revenue"]

    x = [d["date"] for d in data]
    y = [d["revenue"] for d in data]

    plt.figure()
    plt.plot(x, y, marker='o')
    plt.title("Country Revenue Trend")
    plt.xlabel("Date")
    plt.ylabel("Revenue")

    file_path = f"chart_{uuid.uuid4()}.png"
    plt.savefig(file_path)
    plt.close()

    return FileResponse(file_path, media_type="image/png", filename="chart.png")


# =========================
# 📄 DOWNLOAD TABLE (CSV)
# =========================
@router.get("/table")
def download_table(
    db: Session = Depends(get_db),
    tenant=Depends(get_current_tenant)
):
    data = FinancialService.get_tables(db, tenant)["revenue_summary"]

    file_path = f"table_{uuid.uuid4()}.csv"

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(["Branch", "Revenue"])

        # Data
        for row in data:
            writer.writerow([row["branch"], row["revenue"]])

    return FileResponse(file_path, media_type="text/csv", filename="table.csv")


# =========================
# ROOT (OPTIONAL)
# =========================
@router.get("/")
def root_download():
    return {
        "message": "Use /chart or /table endpoint"
    }