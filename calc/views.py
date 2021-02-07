from django.shortcuts import render
from . import views
from django.http import HttpResponse
# Create your views here.

def home(request):
    return render(request,'home.html',{'name':'akshay'})

def add(request):
    val1=int(request.POST["no1"]) *\post method is used to add a value
    val2=int(request.POST["no2"])
    result=val1 + val2
    return render(request,'result.html',{'result':result})
