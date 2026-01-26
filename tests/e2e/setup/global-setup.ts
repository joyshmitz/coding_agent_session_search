import { execSync } from 'child_process';
import { existsSync, mkdirSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Global setup for HTML export E2E tests.
 * Generates test HTML exports from fixture JSONL files before tests run.
 */
async function globalSetup() {
  const projectRoot = path.resolve(__dirname, '../../..');
  const exportDir = path.resolve(__dirname, '../exports');
  const fixturesDir = path.resolve(projectRoot, 'tests/fixtures/html_export/real_sessions');

  // Ensure export directory exists
  if (!existsSync(exportDir)) {
    mkdirSync(exportDir, { recursive: true });
  }

  // Build the Rust CLI if needed
  console.log('Building cass CLI...');
  try {
    execSync('cargo build --release', { cwd: projectRoot, stdio: 'inherit' });
  } catch {
    console.warn('Cargo build failed, trying with existing binary...');
  }

  // Find the cass binary - check CARGO_TARGET_DIR or common locations
  const possiblePaths = [
    process.env.CARGO_TARGET_DIR ? path.join(process.env.CARGO_TARGET_DIR, 'release/cass') : null,
    '/data/tmp/cargo-target/release/cass',
    path.join(projectRoot, 'target/release/cass'),
  ].filter(Boolean) as string[];

  let cassPath = '';
  for (const p of possiblePaths) {
    if (existsSync(p)) {
      cassPath = p;
      break;
    }
  }

  if (!cassPath) {
    throw new Error(`Could not find cass binary. Checked: ${possiblePaths.join(', ')}`);
  }

  console.log(`Using cass binary: ${cassPath}`);

  // Generate test exports
  const exports = [
    {
      name: 'test-basic',
      fixture: 'claude_code_auth_fix.jsonl',
      args: [],
    },
    {
      name: 'test-encrypted',
      fixture: 'claude_code_auth_fix.jsonl',
      args: ['--encrypt', '--password', 'test-password-123'],
    },
    {
      name: 'test-tool-calls',
      fixture: 'cursor_refactoring.jsonl',
      args: [],
    },
    {
      name: 'test-large',
      fixture: '../edge_cases/large_session.jsonl',
      args: [],
    },
    {
      name: 'test-unicode',
      fixture: '../edge_cases/unicode_heavy.jsonl',
      args: [],
    },
    {
      name: 'test-no-cdn',
      fixture: 'claude_code_auth_fix.jsonl',
      args: ['--no-cdns'],
    },
  ];

  // Write environment file for tests
  const envContent: Record<string, string> = {
    TEST_EXPORTS_DIR: exportDir,
    TEST_EXPORT_PASSWORD: 'test-password-123',
  };

  for (const { name, fixture, args } of exports) {
    const fixturePath = path.join(fixturesDir, fixture);
    const outputPath = path.join(exportDir, `${name}.html`);

    console.log(`Generating ${name}.html from ${fixture}...`);

    try {
      // Use the CLI to generate export
      const cmd = [
        cassPath,
        'export-html',
        fixturePath,
        '--output-dir', path.dirname(outputPath),
        '--filename', path.basename(outputPath),
        ...args,
      ].join(' ');

      execSync(cmd, { cwd: projectRoot, stdio: 'pipe' });
      envContent[`TEST_EXPORT_${name.toUpperCase().replace(/-/g, '_')}`] = outputPath;
      console.log(`  -> ${outputPath}`);
    } catch (err) {
      console.error(`Failed to generate ${name}:`, err);
      // Create a placeholder file so tests can check for its existence
      writeFileSync(outputPath, `<!-- Export generation failed for ${name} -->`);
    }
  }

  // Write environment file
  const envPath = path.join(__dirname, '../.env.test');
  writeFileSync(
    envPath,
    Object.entries(envContent)
      .map(([k, v]) => `${k}=${v}`)
      .join('\n')
  );

  console.log('\nE2E test setup complete!');
  console.log(`Exports directory: ${exportDir}`);
  console.log(`Environment file: ${envPath}`);
}

export default globalSetup;
