import json

from django.core.paginator import Paginator
from django.db import connection
from django.db.models import Avg, Sum
from django.http import JsonResponse
from django.shortcuts import render

from .models import DailySalesPrediction


def overview(request):
    qs = DailySalesPrediction.objects.all()
    count = qs.count()

    if count == 0:
        return render(request, "dashboard/overview.html", {"has_data": False})

    totals = qs.aggregate(
        total_actual=Sum("actual_revenue"),
        total_predicted=Sum("predicted_revenue"),
        avg_actual=Avg("actual_revenue"),
        avg_predicted=Avg("predicted_revenue"),
    )

    # Mean Absolute Percentage Error across all rows
    errors = [row.abs_percent_error for row in qs]
    mape = sum(errors) / len(errors) if errors else 0

    best_day = min(qs, key=lambda r: r.abs_percent_error)
    worst_day = max(qs, key=lambda r: r.abs_percent_error)

    context = {
        "has_data": True,
        "count": count,
        "date_range": (qs.order_by("date").first().date, qs.order_by("date").last().date),
        "total_actual": totals["total_actual"],
        "total_predicted": totals["total_predicted"],
        "avg_actual": totals["avg_actual"],
        "avg_predicted": totals["avg_predicted"],
        "mape": mape,
        "best_day": best_day,
        "worst_day": worst_day,
    }
    return render(request, "dashboard/overview.html", context)


def predictions_table(request):
    qs = DailySalesPrediction.objects.all().order_by("-date")

    paginator = Paginator(qs, 30)  # 30 rows per page
    page_number = request.GET.get("page", 1)
    page_obj = paginator.get_page(page_number)

    return render(request, "dashboard/predictions.html", {"page_obj": page_obj})


def charts(request):
    qs = DailySalesPrediction.objects.all().order_by("date")

    dates = [row.date.isoformat() for row in qs]
    actual = [row.actual_revenue for row in qs]
    predicted = [row.predicted_revenue for row in qs]
    errors = [round(row.error, 2) for row in qs]

    # Day-of-week averages (1=Sunday .. 7=Saturday, matching Spark's dayofweek())
    day_names = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    dow_actual_totals = {d: [] for d in range(1, 8)}
    dow_predicted_totals = {d: [] for d in range(1, 8)}
    for row in qs:
        dow_actual_totals[row.day_of_week_num].append(row.actual_revenue)
        dow_predicted_totals[row.day_of_week_num].append(row.predicted_revenue)

    dow_order = [2, 3, 4, 5, 6, 7, 1]  # Mon..Sun for a natural week view
    dow_labels = [day_names[d] for d in dow_order]
    dow_avg_actual = [
        round(sum(dow_actual_totals[d]) / len(dow_actual_totals[d]), 2) if dow_actual_totals[d] else 0
        for d in dow_order
    ]
    dow_avg_predicted = [
        round(sum(dow_predicted_totals[d]) / len(dow_predicted_totals[d]), 2) if dow_predicted_totals[d] else 0
        for d in dow_order
    ]

    # Scatter points for actual vs predicted (accuracy visualization)
    scatter_points = [{"x": a, "y": p} for a, p in zip(actual, predicted)]

    context = {
        "dates_json": json.dumps(dates),
        "actual_json": json.dumps(actual),
        "predicted_json": json.dumps(predicted),
        "errors_json": json.dumps(errors),
        "dow_labels_json": json.dumps(dow_labels),
        "dow_avg_actual_json": json.dumps(dow_avg_actual),
        "dow_avg_predicted_json": json.dumps(dow_avg_predicted),
        "scatter_points_json": json.dumps(scatter_points),
        "has_data": len(dates) > 0,
    }
    return render(request, "dashboard/charts.html", context)


def live_page(request):
    """Renders the shell page; actual data is fetched via JS polling live_data()."""
    return render(request, "dashboard/live.html")


def _table_exists(table_name):
    return table_name in connection.introspection.table_names()


def live_data(request):
    """
    JSON endpoint polled every few seconds by the Live page.
    Reads directly from the two tables the streaming consumer
    (spark_streaming_consumer.py) writes to via foreachBatch:
      - live_region_category_totals (overwritten each micro-batch = current full state)
      - live_recent_transactions (appended each micro-batch)
    Returns gracefully with has_data=False if the streaming script hasn't
    run yet (tables won't exist until its first micro-batch completes).
    """
    agg_table = "live_region_category_totals"
    raw_table = "live_recent_transactions"

    if not _table_exists(agg_table) or not _table_exists(raw_table):
        return JsonResponse({"has_data": False})

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT region, category, total_revenue, total_units_sold "
            f"FROM {agg_table} ORDER BY total_revenue DESC LIMIT 15"
        )
        columns = [c[0] for c in cursor.description]
        aggregates = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.execute(
            f"SELECT transaction_id, date, store_id, region, product_id, "
            f"category, units_sold, unit_price, sales_amount, received_at "
            f"FROM {raw_table} ORDER BY received_at DESC LIMIT 20"
        )
        columns = [c[0] for c in cursor.description]
        recent = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.execute(f"SELECT COUNT(*) FROM {raw_table}")
        total_transactions = cursor.fetchone()[0]

        cursor.execute(f"SELECT COALESCE(SUM(total_revenue), 0) FROM {agg_table}")
        total_revenue = cursor.fetchone()[0]

    # Convert non-JSON-safe types (date, datetime, Decimal) to strings/floats
    for row in recent:
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif value is not None and not isinstance(value, (int, float, str)):
                row[key] = float(value)

    for row in aggregates:
        for key, value in row.items():
            if value is not None and not isinstance(value, (int, float, str)):
                row[key] = float(value)

    return JsonResponse({
        "has_data": True,
        "aggregates": aggregates,
        "recent": recent,
        "total_transactions": total_transactions,
        "total_revenue": float(total_revenue),
    })
