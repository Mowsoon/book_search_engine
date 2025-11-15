from django.contrib import admin
from django.urls import path
from gutenberg_api import views
from core.views import HomeView

urlpatterns = [
    path('admin/', admin.site.urls),

# --- The Frontend ---
    path('', HomeView.as_view(), name='home'),

    # API Endpoints
    path('api/search',
         views.SimpleSearchView.as_view(), name='search'),

    path('api/search/advanced',
         views.AdvancedSearchView.as_view(), name='search_advanced'),

    path('api/book/<int:book_id>/suggestions',
         views.SuggestionView.as_view(), name='suggestions'),

    path('api/book/<int:book_id>/content',
         views.BookContentView.as_view(), name='book_content'),
]