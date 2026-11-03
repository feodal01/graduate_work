from django.http import JsonResponse
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.views.generic.list import BaseListView
from django.views.generic.detail import BaseDetailView

from movies.models import FilmWork, Roles


class MoviesApiMixin:
    model = FilmWork
    http_method_names = ['get']

    def _film_to_dict(self, film):
        return {
            'id': str(film.id),
            'title': film.title,
            'description': film.description,
            'creation_date': film.creation_date.isoformat() if film.creation_date else None,
            'rating': film.rating,
            'type': film.type,
            'genres': film.genres_names or [],
            'actors': film.actors or [],
            'directors': film.directors or [],
            'writers': film.writers or [],
        }

    def get_queryset(self):
        queryset = FilmWork.objects.prefetch_related('genres', 'persons').annotate(  # pyright: ignore[reportAttributeAccessIssue]
            genres_names=ArrayAgg('genres__name', distinct=True),
            actors=ArrayAgg(
                'persons__full_name',
                filter=Q(persons__personfilmwork__role=Roles.ACTOR),
                distinct=True
            ),
            directors=ArrayAgg(
                'persons__full_name',
                filter=Q(persons__personfilmwork__role=Roles.DIRECTOR),
                distinct=True
            ),
            writers=ArrayAgg(
                'persons__full_name',
                filter=Q(persons__personfilmwork__role=Roles.WRITER),
                distinct=True
            ),
        )
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    paginate_by = 50

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset, self.paginate_by
        )
        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': [self._film_to_dict(film) for film in queryset],
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    pk_url_kwarg = 'id'

    def get_context_data(self, **kwargs):
        return  self._film_to_dict(self.object) # Словарь с данными объекта


