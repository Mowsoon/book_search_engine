from django.contrib import admin
from django.urls import path
from gutenberg_api import views

urlpatterns = [
    path('admin/', admin.site.urls),

    # API Endpoints
    path('api/search',
         views.SimpleSearchView.as_view(), name='search'),

    path('api/search/advanced',
         views.AdvancedSearchView.as_view(), name='search_advanced'),

    path('api/book/<int:book_id>/suggestions',
         views.SuggestionView.as_view(), name='suggestions'),
]