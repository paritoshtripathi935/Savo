from re import template
from django.views.generic import TemplateView

class HomePageView(TemplateView):
    template_name = 'index.html'

class AboutPageview(TemplateView):
    template_name = 'about.html'

class ContactPageview(TemplateView):
    template_name = 'contact.html'

class ElementPageview(TemplateView):
    template_name = 'elements.html'