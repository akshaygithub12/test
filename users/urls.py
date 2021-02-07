# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 19:41:14 2021

@author: gautamg1
"""

from django.urls import path

from . import views

urlpatterns=[
    path('register',views.register,name='register'),
    ]
