from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from example.admin_site import integrated_admin_site

from .models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


@admin.register(Genre, site=integrated_admin_site)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ('name',)


@admin.register(Person, site=integrated_admin_site)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ('full_name',)


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    autocomplete_fields = ('genre',)


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    autocomplete_fields = ('person',)


@admin.register(FilmWork, site=integrated_admin_site)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)
    list_display = ('title', 'type', 'creation_date', 'rating', 'get_genres')
    list_filter = ('type', 'genres')
    search_fields = ('title', 'description', 'id')

    def get_queryset(self, request):
        queryset = super().get_queryset(request).prefetch_related('genres')
        return queryset

    def get_genres(self, obj):
        return ','.join([genre.name for genre in obj.genres.all()])

    get_genres.short_description = _('genres')
