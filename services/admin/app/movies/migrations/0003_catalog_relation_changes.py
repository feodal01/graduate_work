"""Let ETL discover changes to genre membership through film_work.modified."""

from django.db import migrations

FORWARD_SQL = """
CREATE OR REPLACE FUNCTION content.touch_film_genre_membership()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.film_work_id = NEW.film_work_id
       AND OLD.genre_id = NEW.genre_id THEN
        RETURN NULL;
    END IF;
    IF TG_OP IN ('DELETE', 'UPDATE') THEN
        UPDATE content.film_work SET modified = clock_timestamp()
        WHERE id = OLD.film_work_id;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        UPDATE content.film_work SET modified = clock_timestamp()
        WHERE id = NEW.film_work_id;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER film_genre_membership_changed
AFTER INSERT OR UPDATE OR DELETE ON content.genre_film_work
FOR EACH ROW EXECUTE FUNCTION content.touch_film_genre_membership();
"""

REVERSE_SQL = """
DROP TRIGGER IF EXISTS film_genre_membership_changed ON content.genre_film_work;
DROP FUNCTION IF EXISTS content.touch_film_genre_membership();
"""


class Migration(migrations.Migration):
    dependencies = [('movies', '0002_alter_personfilmwork_role')]
    operations = [migrations.RunSQL(FORWARD_SQL, REVERSE_SQL)]
