from django.views.generic import TemplateView
from django.shortcuts import redirect, render
from django.shortcuts import HttpResponse
from django.contrib.auth.models import User
from django.contrib import messages

class HomePageView(TemplateView):
    template_name = 'index.html'

class AboutPageview(TemplateView):
    template_name = 'about.html'

class ContactPageview(TemplateView):
    template_name = 'contact.html'

class ElementPageview(TemplateView):
    template_name = 'elements.html'

class SignInPageview(TemplateView):
    template_name = 'sign-in.html'

class SignUpPageView(TemplateView):
    template_name = "sign-up.html"

