// Small extension kept with the backend because the main cashier bundle is
// maintained upstream. No merchant or wallet data is sent off origin.
(() => {
  'use strict';
  let lastPath = '', notice;
  function updateNavigation(path) {
    document.getElementById('collection-admin-link')?.remove();
    const signedIn = document.cookie.split('; ').some(v => v.startsWith('access_token='));
    if (!signedIn || path.startsWith('/sign-in') || path.startsWith('/cashier/')) {
      document.querySelectorAll('[data-collection-nav]').forEach(item => item.remove());
      return;
    }
    // The upstream app renders desktop and mobile sidebars separately. Attach
    // to each rendered menu, including sidebars mounted after this script.
    for (const template of document.querySelectorAll('[data-sidebar="menu-button"][href="/orders"]')) {
      const menuItem = template.closest('[data-sidebar="menu-item"]');
      const menu = menuItem?.parentElement;
      if (!menu || menu.querySelector('[data-collection-nav]')) continue;
      const item = document.createElement('li');
      item.className = menuItem.className;
      item.setAttribute('data-sidebar', 'menu-item');
      item.setAttribute('data-slot', 'sidebar-menu-item');
      item.setAttribute('data-collection-nav', '');
      const link = document.createElement('a');
      link.href = '/admin/collections';
      link.className = template.className;
      link.setAttribute('data-sidebar', 'menu-button');
      link.setAttribute('data-slot', 'sidebar-menu-button');
      link.setAttribute('data-size', 'default');
      link.setAttribute('data-active', 'false');
      link.setAttribute('aria-label', '到账与异常记录');
      link.title = '到账与异常记录';
      link.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M8 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-3"/><path d="M16 3h5v5M10 14 21 3M8 10H7M7 14h1M7 18h8"/></svg><span>到账与异常记录</span>';
      item.append(link);
      menuItem.after(item);
    }
  }
  async function update() {
    const path = location.pathname;
    updateNavigation(path);
    if (path === lastPath) return;
    lastPath = path;
    if (notice) { notice.remove(); notice = null; }
    const match = path.match(/^\/cashier\/([^/]+)$/);
    if (match) {
      try {
        const r = await fetch('/pay/checkout-counter-resp/' + encodeURIComponent(decodeURIComponent(match[1])));
        const body = await r.json();
        if (path !== location.pathname || body.data?.collection_mode !== 'exclusive_actual') return;
        notice = document.createElement('aside');
        notice.setAttribute('role', 'note');
        notice.textContent = '本订单按首次有效到账金额结算，少付或多付均会结束订单。请核对网络和币种，在倒计时结束前完成支付；不要重复付款或保存此地址供下次使用。超时款需联系客服人工处理。';
        notice.style.cssText = 'padding:12px 20px;background:#fffbeb;color:#78350f;font:14px/1.6 system-ui;border-bottom:1px solid #fde68a;';
        document.body.prepend(notice);
      } catch { lastPath = ''; }
    }
  }
  let pending = false;
  // Mobile navigation is rendered in a portal outside #app.
  if (document.body) new MutationObserver(() => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(() => { pending = false; update(); });
  }).observe(document.body, { childList: true, subtree: true });
  update(); window.addEventListener('popstate', update); setInterval(update, 1000);
})();
