const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
assert.equal(scripts.length, 1, 'the demo has one inline script');
const script = scripts[0][1];

test('inline browser script passes node --check', () => {
  const result = spawnSync(process.execPath, ['--check', '-'], {
    input: script,
    encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
});

test('WebSocket ArrayBuffer audio is scheduled and acknowledged after playback', () => {
  const elements = new Map();
  const sent = [];
  const sources = [];
  const audioBuffers = [];
  let socket;
  class FakeWebSocket {
    constructor(url) {
      assert.equal(url, 'ws://localhost:8080/api/voice/ws');
      this.readyState = FakeWebSocket.OPEN;
      socket = this;
    }
    send(message) { sent.push(JSON.parse(message)); }
  }
  FakeWebSocket.OPEN = 1;
  const audioCtx = {
    currentTime: 0,
    createBuffer(channels, length, sampleRate) {
      assert.equal(channels, 1);
      assert.equal(sampleRate, 24000);
      const buffer = {
        copyToChannel(samples) {
          assert.equal(samples.length, length);
          audioBuffers.push(Array.from(samples));
        },
      };
      return buffer;
    },
    createBufferSource() {
      const source = {
        connect() {},
        start() {},
      };
      sources.push(source);
      return source;
    },
  };
  const document = {
    getElementById(id) {
      if (!elements.has(id)) {
        elements.set(id, { appendChild() {}, addEventListener() {} });
      }
      return elements.get(id);
    },
    createElement() { return {}; },
  };
  const context = vm.createContext({
    document,
    window: { addEventListener() {} },
    WebSocket: FakeWebSocket,
    location: { protocol: 'http:', host: 'localhost:8080' },
    audioCtx,
    gain: {},
  });
  vm.runInContext(script, context, { filename: 'web/index.html' });
  vm.runInContext('playbackCtx = audioCtx; gainNode = gain;', context);
  context.startSession();
  assert.equal(socket.binaryType, 'arraybuffer');
  assert.equal(typeof socket.onmessage, 'function');

  socket.onmessage({ data: JSON.stringify({
    type: 'speech_started', speech_id: 'speech-1', speech_epoch: 3, sample_rate: 24000,
  }) });

  const audio = new ArrayBuffer(12);
  const view = new DataView(audio);
  view.setBigUint64(0, 7n, true);
  view.setInt16(8, 16384, true);
  view.setInt16(10, -16384, true);
  socket.onmessage({ data: audio });

  assert.deepEqual(audioBuffers, [[0.5, -0.5]]);
  assert.equal(sources.length, 1);
  assert.deepEqual(sent, [], 'do not ACK until playback ends');

  socket.onmessage({ data: JSON.stringify({ type: 'speech_done' }) });
  assert.deepEqual(sent, [], 'speech_done does not ACK an unfinished chunk');
  sources[0].onended();
  assert.deepEqual(sent, [
    {
      type: 'playback_chunk_completed',
      speech_id: 'speech-1', speech_epoch: 3, sequence: 7, played_samples: 2,
    },
    { type: 'playback_completed', speech_id: 'speech-1', speech_epoch: 3 },
  ]);
});
