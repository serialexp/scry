//! Password gate shown (browser only) until a valid session exists.
//! Reads/writes the shared store directly — no props.

import { createSignal, Show, type Component } from "solid-js";

import { login } from "../store";

const LoginForm: Component = () => {
  const [password, setPassword] = createSignal("");
  const [error, setError] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);

  async function submit(e: Event) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const ok = await login(password());
      if (!ok) setError("Incorrect password.");
    } catch {
      setError("Could not reach the server.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <form class="login-form" onSubmit={submit}>
      <h2>Sign in</h2>
      <div class="field">
        <label for="password">Password</label>
        <input
          id="password"
          type="password"
          autofocus
          value={password()}
          onInput={(e) => setPassword(e.currentTarget.value)}
        />
      </div>
      <Show when={error()}>
        <p class="login-error">{error()}</p>
      </Show>
      <button type="submit" class="run" disabled={busy() || password() === ""}>
        {busy() ? "Signing in…" : "Sign in"}
      </button>
    </form>
  );
};

export default LoginForm;
