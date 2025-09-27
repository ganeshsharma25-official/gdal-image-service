from django.urls import path
from .views import NDVIProcessingView, NDWIProcessingView

urlpatterns = [
    path('process-ndvi/', NDVIProcessingView.as_view(), name='process-ndvi'),
    path('process-ndwi/', NDWIProcessingView.as_view(), name='process-ndwi'),
]
