from django.shortcuts import render

def index(request):
	return render(request, 'map/body-extend.html')

