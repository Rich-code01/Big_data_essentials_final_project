from django.db import models


class DailySalesPrediction(models.Model):
    """
    Maps to the 'daily_sales_predictions' table, which is created and
    populated by train_and_predict.py (the PySpark MLlib training script).

    managed = False tells Django not to create/alter/drop this table via
    migrations — Spark owns it. Django just reads from it.
    """
    date = models.DateField(primary_key=True)
    actual_revenue = models.FloatField()
    predicted_revenue = models.FloatField()
    day_of_week_num = models.IntegerField()
    is_weekend = models.IntegerField()
    is_holiday_season = models.IntegerField()

    class Meta:
        managed = False
        db_table = "daily_sales_predictions"
        ordering = ["date"]

    @property
    def error(self):
        return self.predicted_revenue - self.actual_revenue

    @property
    def abs_percent_error(self):
        if self.actual_revenue == 0:
            return 0
        return abs(self.error) / self.actual_revenue * 100

    def __str__(self):
        return f"{self.date}: actual={self.actual_revenue:.2f} predicted={self.predicted_revenue:.2f}"
