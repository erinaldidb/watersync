import { expect, test } from '@playwright/test';

test('control plane shell and primary navigation load', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText('WaterSync Control Plane')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Pipeline health at a glance' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Configuration' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Watermarks' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Jobs' })).toBeVisible();
});

test('configuration screen exposes lookup and create actions', async ({ page }) => {
  await page.goto('/config');
  await expect(page.getByRole('heading', { name: 'Ingestion configuration' })).toBeVisible();
  await expect(page.getByLabel('Filter configurations')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Add entry' })).toBeVisible();
});
