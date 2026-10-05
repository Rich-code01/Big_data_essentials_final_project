from django.urls import path
from . import views

urlpatterns = [
    path("", views.overview, name="overview"),
    path("predictions/", views.predictions_table, name="predictions"),
    path("charts/", views.charts, name="charts"),
]
