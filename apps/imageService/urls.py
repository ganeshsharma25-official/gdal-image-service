from django.urls import path
from .views import NDVIProcessingView

urlpatterns = [
    path('process-ndvi/', NDVIProcessingView.as_view(), name='process-ndvi'),
]
