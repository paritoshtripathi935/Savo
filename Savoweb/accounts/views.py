import re
from django.shortcuts import render

# Create your views here.
# This is the views files user login


def register(request):
    return render(request, 'sign-up.html')