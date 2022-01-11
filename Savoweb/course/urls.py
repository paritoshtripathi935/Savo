from django.urls import path
from . import views

urlpatterns = [
    path('', views.HomePageView.as_view(), name='index'),
    path('about/', views.AboutPageview.as_view(), name='about'),
    path('contact/',views.ContactPageview.as_view(), name='contact'),
    path('elements/',views.ElementPageview.as_view(), name='elements'),
]
