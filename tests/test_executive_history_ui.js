const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const template = fs.readFileSync('apps/templates/home/executive_plan.html', 'utf8');
const context = vm.createContext({});
for (const [start, end] of [['function escapeHistoryHtml', 'function formatHistoryDate'], ['function historyTeamLabel', 'function renderRecommendationHistoryChart']]) {
  vm.runInContext(template.slice(template.indexOf(start), template.indexOf(end)), context);
}
test('groups the same advisor type across work packages with counts and closed details', () => {
  const item = {advisor_id: 'Missing indexes', source: 'global', title: 'Finding', team:'DEV', corrected_at:'2026-10-25', recommendation_sql:'SELECT 1;'};
  const html = context.historyPackagesHtml([{recommendations:[item]}, {recommendations:[{...item, scope_name:'orders'}]}], 'No longer detected');
  assert.equal((html.match(/<details/g)||[]).length, 1);
  assert.match(html, /2 recommendations/);
  assert.match(html, /orders/);
  assert.match(html, /SELECT 1;/);
  assert.doesNotMatch(html, /2026-10-25|No longer detected|<details[^>]* open/);
});
test('keeps separate source types, escapes content, and retains changed status', () => {
  const html = context.historyPackagesHtml([{recommendations:[{advisor_id:'<unsafe>', source:'a', modified_at:'date'}, {advisor_id:'<unsafe>', source:'b'}]}], 'Detected');
  assert.equal((html.match(/<details/g)||[]).length, 2);
  assert.match(html, /&lt;unsafe&gt;/);
  assert.match(html, /Changed/);
  assert.match(html, /1 recommendation</);
  assert.equal(context.historyPackagesHtml([], 'Detected'), '');
});
test('expanded findings show targets without repeating type titles and use a quiet current-state badge', () => {
  const item = {advisor_id:'autovacuum_vacuum_scale_factor', title:'Review autovacuum parameter autovacuum_vacuum_scale_factor', scope_name:'autovacuum_vacuum_scale_factor', live_comparison:true};
  const html = context.historyFindingHtml(item, 'No longer detected');
  assert.doesNotMatch(html, /Review autovacuum|history-change-title|autovacuum_vacuum_scale_factor/);
  assert.match(html, /history-event history-current-state/);
  assert.match(context.historyFindingHtml({...item, scope_name:'public.orders'}, 'Detected'), /public.orders/);
});
