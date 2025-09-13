const CACHE_NAME = 'my-webpage-cache';

// two ideas here
// 1. i can access html from a browser cache if i am offline or server not reachable
// 2. if i can access html but request failes due to network error, i can save items to local storage
// ... one in local storage, figure out a way to broadcast them once connection restores
// learn more about progessive web apps...
self.addEventListener('install', (event) => {
  console.log("intall event");
  console.log(event);
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        cache.addAll([
        '/', // cache root URL
        'static/dashboard.html',
        '/static',
      ]); console.log(cache);
    }).catch((error) => console.log(error))
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

self.addEventListener('activate', event => {
    const cacheWhitelist = [CACHE_NAME];
    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheWhitelist.indexOf(cacheName) === -1) {
                        return caches.delete(cacheName);
                    }
                })
            );
        })
    );
});
