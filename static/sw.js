const CACHE_NAME = 'my-webpage-cache';

// Install event handler
self.addEventListener('install', (event) => {
  console.log("intall event");
  console.log(event);
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll([
        '/', // cache root URL
        'static/dashboard.html',
        'static/index.html',
      ]))
  );
});

// Fetch event handler
self.addEventListener('fetch', (event) => {
  console.log("fetch intercepted");
  console.log(event);
  event.respondWith(
    caches.match(event.request).then((response) => {
      return response || fetch(event.request);
    })
  );
});
