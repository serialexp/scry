/** The scry mark: a scrying lens with four signal ticks. Uses a unique
 * gradient id per instance so multiple logos on one page don't collide. */
let counter = 0;

export function Logo(props: { size?: number }) {
  const size = props.size ?? 32;
  const id = `scry-logo-${counter++}`;
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 64 64"
      fill="none"
      aria-hidden="true"
      style={{ "flex-shrink": 0 }}
    >
      <defs>
        <linearGradient id={id} x1="8" y1="8" x2="56" y2="56" gradientUnits="userSpaceOnUse">
          <stop stop-color="#3ce8c6" />
          <stop offset="1" stop-color="#8b6cf0" />
        </linearGradient>
      </defs>
      <circle cx="32" cy="32" r="17" stroke={`url(#${id})`} stroke-width="3.5" />
      <circle cx="32" cy="32" r="6.5" fill={`url(#${id})`} />
      <path
        d="M32 6v7M32 51v7M6 32h7M51 32h7"
        stroke={`url(#${id})`}
        stroke-width="3.5"
        stroke-linecap="round"
      />
    </svg>
  );
}
