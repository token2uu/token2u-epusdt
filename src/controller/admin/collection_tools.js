// Small extension kept with the backend because the main cashier bundle is
// maintained upstream. No merchant or wallet data is sent off origin.
(() => {
  'use strict';
  let lastPath = '', notice;
  async function update() {
    const path = location.pathname;
    if (path === lastPath) return;
    lastPath = path;
    if (notice) { notice.remove(); notice = null; }
    document.getElementById('collection-admin-link')?.remove();
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
    } else if (document.cookie.split('; ').some(v => v.startsWith('access_token=')) && !path.startsWith('/sign-in')) {
      const link = document.createElement('a');
      link.id = 'collection-admin-link'; link.href = '/admin/collections'; link.textContent = '到账与异常记录';
      link.style.cssText = 'position:fixed;right:16px;bottom:16px;z-index:40;background:#1e293b;color:#fff;padding:12px 16px;border-radius:8px;font:14px system-ui;box-shadow:0 2px 8px #0002;';
      document.body.append(link);
    }
  }
  update(); window.addEventListener('popstate', update); setInterval(update, 1000);
})();
