from django.urls import path
from . import views

urlpatterns = [
    path('visualizar_dataset/', views.visualizar_dataset, name='visualizar_dataset'),
    path('upload_arff/', views.upload_arff, name='upload_arff'),
]