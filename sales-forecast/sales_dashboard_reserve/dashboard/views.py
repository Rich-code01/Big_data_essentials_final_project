import json

from django.core.paginator import Paginator
from django.db.models import Avg, Sum
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
