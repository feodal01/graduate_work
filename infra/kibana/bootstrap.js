// Идемпотентно создаёт в Kibana data views для логов: app-* и nginx-*.
// Если data view с таким шаблоном уже есть (в том числе созданный вручную),
// он не трогается: скрипт ничего не удаляет и не создаёт дублей.
const KIBANA_URL = process.env.KIBANA_URL || 'http://kibana:5601';
const PATTERNS = ['app-*', 'nginx-*'];
const DEFAULT_PATTERN = 'app-*';

async function kibana(method, path, body) {
  const response = await fetch(`${KIBANA_URL}${path}`, {
    method,
    headers: { 'kbn-xsrf': 'true', 'Content-Type': 'application/json' },
    body: body && JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} ${path}: ${response.status} ${text}`);
  }
  return JSON.parse(text);
}

async function main() {
  const { data_view: existing } = await kibana('GET', '/api/data_views');
  const idByTitle = new Map(existing.map((view) => [view.title, view.id]));

  for (const pattern of PATTERNS) {
    if (idByTitle.has(pattern)) {
      console.log(`Data view ${pattern} уже есть`);
      continue;
    }
    const { data_view: created } = await kibana('POST', '/api/data_views/data_view', {
      data_view: { title: pattern, name: pattern, timeFieldName: '@timestamp' },
    });
    idByTitle.set(pattern, created.id);
    console.log(`Создан data view ${pattern}`);
  }

  // Data view по умолчанию для Discover; выбранный пользователем не перезаписывается.
  await kibana('POST', '/api/data_views/default', {
    data_view_id: idByTitle.get(DEFAULT_PATTERN),
    force: false,
  });
  console.log(`Kibana готова: data views ${PATTERNS.join(', ')}`);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
