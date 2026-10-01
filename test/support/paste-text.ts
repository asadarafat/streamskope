import type userEvent from "@testing-library/user-event";

/** Bulk entry for prerequisite text; use keyboard/type when keystrokes are the behavior. */
export async function pasteText(
  user: ReturnType<typeof userEvent.setup>,
  input: HTMLElement,
  text: string,
): Promise<void> {
  await user.click(input);
  await user.paste(text);
}
