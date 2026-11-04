from pathlib import Path

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / 'templates'
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def render_error_page(
    request: Request,
    *,
    status_code: int,
    message: str,
    title: str = 'Произошла ошибка',
) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name='error.html',
        context={'title': title, 'message': message},
        status_code=status_code,
    )
