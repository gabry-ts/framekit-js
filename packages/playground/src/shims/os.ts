// Browser shim for Node.js 'os' module
// Only the subset used by framekit-js/browser is implemented

export function cpus(): { model: string }[] {
  const count =
    typeof navigator !== 'undefined' && navigator.hardwareConcurrency
      ? navigator.hardwareConcurrency
      : 2;
  return Array.from({ length: count }, () => ({ model: 'browser' }));
}

export function platform(): string {
  return 'browser';
}

export function arch(): string {
  return 'unknown';
}

export function tmpdir(): string {
  return '/tmp';
}

export const EOL = '\n';

export default { cpus, platform, arch, tmpdir, EOL };
