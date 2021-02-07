# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 20:22:41 2021

@author: gautamg1
"""

from django.urls import path

from . import views
urlpatterns=[
    path('',views.home,name='home'),
    path('add',views.add,name='add')
    ]