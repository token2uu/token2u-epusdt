import { xm as getLocale } from './messages-Cl9iE86Z.js';

const copy = {
  'zh-CN': ['付费回退', '普通节点失败、不可用或冷却时使用；恢复后自动优先普通节点。'],
  'zh-TW': ['付費回退', '一般節點失敗、無法使用或冷卻時使用；恢復後自動優先一般節點。'],
  'en-US': ['Paid fallback', 'Used when ordinary nodes fail, are unavailable or cooling down. Ordinary nodes are preferred again after recovery.'],
  'ja-JP': ['有料フォールバック', '通常ノードの障害・利用不可・クールダウン時に使用します。復旧後は通常ノードを優先します。'],
  'ko-KR': ['유료 대체 노드', '일반 노드가 실패하거나 사용할 수 없거나 대기 중일 때 사용합니다. 복구 후 일반 노드를 우선합니다.'],
  'ru-RU': ['Платный резерв', 'Используется при сбое, недоступности или ограничении обычных узлов. После восстановления обычные узлы снова имеют приоритет.'],
};

export function paidFallbackLabel() {
  return (copy[getLocale()] ?? copy['en-US'])[0];
}

export function paidFallbackDescription() {
  return (copy[getLocale()] ?? copy['en-US'])[1];
}
