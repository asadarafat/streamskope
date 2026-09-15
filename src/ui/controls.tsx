import Accordion, { type AccordionProps } from "@mui/material/Accordion";
import AccordionDetails, { type AccordionDetailsProps } from "@mui/material/AccordionDetails";
import AccordionSummary, { type AccordionSummaryProps } from "@mui/material/AccordionSummary";
import Alert, { type AlertProps } from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button, { type ButtonProps } from "@mui/material/Button";
import Checkbox, { type CheckboxProps } from "@mui/material/Checkbox";
import Dialog, { type DialogProps } from "@mui/material/Dialog";
import DialogActions, { type DialogActionsProps } from "@mui/material/DialogActions";
import DialogContent, { type DialogContentProps } from "@mui/material/DialogContent";
import DialogTitle, { type DialogTitleProps } from "@mui/material/DialogTitle";
import FormControl, { type FormControlProps } from "@mui/material/FormControl";
import FormControlLabel, { type FormControlLabelProps } from "@mui/material/FormControlLabel";
import FormLabel, { type FormLabelProps } from "@mui/material/FormLabel";
import IconButton, { type IconButtonProps } from "@mui/material/IconButton";
import InputLabel, { type InputLabelProps } from "@mui/material/InputLabel";
import ListItemButton, { type ListItemButtonProps } from "@mui/material/ListItemButton";
import Menu, { type MenuProps } from "@mui/material/Menu";
import MenuItem, { type MenuItemProps } from "@mui/material/MenuItem";
import Radio, { type RadioProps } from "@mui/material/Radio";
import Select, { type SelectProps } from "@mui/material/Select";
import Switch, { type SwitchProps } from "@mui/material/Switch";
import Tab, { type TabProps } from "@mui/material/Tab";
import Tabs, { type TabsProps } from "@mui/material/Tabs";
import TextField, { type TextFieldProps } from "@mui/material/TextField";
import ToggleButton, { type ToggleButtonProps } from "@mui/material/ToggleButton";
import ToggleButtonGroup, { type ToggleButtonGroupProps } from "@mui/material/ToggleButtonGroup";
import Tooltip, { type TooltipProps } from "@mui/material/Tooltip";
import {
  forwardRef,
  type AnchorHTMLAttributes,
  type ElementType,
  type KeyboardEvent,
  type RefObject,
  useEffect,
} from "react";

export const StudioButton = forwardRef<HTMLButtonElement, ButtonProps>(
  function StudioButton(properties, reference) {
    return <Button {...properties} ref={reference} />;
  },
);

type StudioIconButtonProperties = IconButtonProps &
  Pick<AnchorHTMLAttributes<HTMLAnchorElement>, "href" | "rel" | "target"> & {
    component?: ElementType;
    title?: string;
  };

export const StudioIconButton = forwardRef<HTMLButtonElement, StudioIconButtonProperties>(
  function StudioIconButton({ title, ...properties }, reference) {
    const button = <IconButton {...properties} data-studio-tooltip={title} ref={reference} />;

    if (title === undefined) {
      return button;
    }
    if (properties.disabled === true) {
      return (
        <Tooltip
          describeChild
          slotProps={{
            popper: { disablePortal: true },
            transition: { timeout: 0 },
          }}
          title={title}
        >
          <Box component="span" sx={{ display: "inline-flex" }}>
            {button}
          </Box>
        </Tooltip>
      );
    }
    return (
      <Tooltip
        describeChild
        slotProps={{
          popper: { disablePortal: true },
          transition: { timeout: 0 },
        }}
        title={title}
      >
        {button}
      </Tooltip>
    );
  },
);

export function StudioTextField({
  "aria-describedby": ariaDescribedBy,
  "aria-errormessage": ariaErrorMessage,
  "aria-label": ariaLabel,
  inputMode,
  onKeyDown,
  slotProps,
  ...properties
}: TextFieldProps): React.JSX.Element {
  const htmlInput = typeof slotProps?.htmlInput === "object" ? slotProps.htmlInput : {};
  return (
    <TextField
      fullWidth
      size="small"
      variant="outlined"
      {...properties}
      slotProps={{
        ...slotProps,
        htmlInput: {
          ...htmlInput,
          ...(ariaDescribedBy === undefined ? {} : { "aria-describedby": ariaDescribedBy }),
          ...(ariaErrorMessage === undefined ? {} : { "aria-errormessage": ariaErrorMessage }),
          ...(ariaLabel === undefined ? {} : { "aria-label": ariaLabel }),
          ...(inputMode === undefined ? {} : { inputMode }),
          ...(onKeyDown === undefined ? {} : { onKeyDown }),
        },
      }}
    />
  );
}

export function StudioSelect<Value = unknown>(properties: SelectProps<Value>): React.JSX.Element {
  return <Select fullWidth size="small" {...properties} />;
}

export function StudioListItemButton(properties: ListItemButtonProps): React.JSX.Element {
  return <ListItemButton {...properties} />;
}

export function StudioFormControl(properties: FormControlProps): React.JSX.Element {
  return <FormControl fullWidth size="small" {...properties} />;
}

export function StudioFormLabel(properties: FormLabelProps): React.JSX.Element {
  return <FormLabel {...properties} />;
}

export function StudioInputLabel(properties: InputLabelProps): React.JSX.Element {
  return <InputLabel {...properties} />;
}

export function StudioCheckbox({
  "aria-label": ariaLabel,
  slotProps,
  ...properties
}: CheckboxProps): React.JSX.Element {
  const input = typeof slotProps?.input === "object" ? slotProps.input : {};
  return (
    <Checkbox
      size="small"
      {...properties}
      slotProps={{ ...slotProps, input: { ...input, "aria-label": ariaLabel } }}
    />
  );
}

export function StudioSwitch(properties: SwitchProps): React.JSX.Element {
  return <Switch size="small" {...properties} />;
}

export function StudioRadio(properties: RadioProps): React.JSX.Element {
  return <Radio size="small" {...properties} />;
}

export function StudioLabeledControl(properties: FormControlLabelProps): React.JSX.Element {
  return <FormControlLabel {...properties} />;
}

function useInitialFocus(
  open: boolean,
  initialFocusReference?: RefObject<HTMLElement | null>,
): void {
  useEffect(() => {
    if (!open || initialFocusReference === undefined) {
      return undefined;
    }
    initialFocusReference.current?.focus();
    return undefined;
  }, [initialFocusReference, open]);
}

type StudioDialogProperties = DialogProps & {
  initialFocusRef?: RefObject<HTMLElement | null>;
};

export function StudioDialog({
  initialFocusRef,
  open,
  ...properties
}: StudioDialogProperties): React.JSX.Element {
  useInitialFocus(open, initialFocusRef);
  return <Dialog fullWidth maxWidth="sm" open={open} {...properties} />;
}

export function StudioDialogTitle(properties: DialogTitleProps): React.JSX.Element {
  return <DialogTitle {...properties} />;
}

export function StudioDialogContent(properties: DialogContentProps): React.JSX.Element {
  return <DialogContent dividers {...properties} />;
}

export function StudioDialogActions(properties: DialogActionsProps): React.JSX.Element {
  return <DialogActions {...properties} />;
}

type StudioMenuItemProperties = MenuItemProps &
  Pick<AnchorHTMLAttributes<HTMLAnchorElement>, "href" | "rel" | "target"> & {
    component?: ElementType;
  };

export function StudioMenuItem(properties: StudioMenuItemProperties): React.JSX.Element {
  return <MenuItem {...properties} />;
}

export function StudioMenu(properties: MenuProps): React.JSX.Element {
  return <Menu {...properties} />;
}

export function StudioAlert(properties: AlertProps): React.JSX.Element {
  return <Alert {...properties} />;
}

export function StudioTabs(properties: TabsProps): React.JSX.Element {
  return <Tabs {...properties} />;
}

export function StudioTab(properties: TabProps): React.JSX.Element {
  return <Tab {...properties} />;
}

export function StudioAccordion(properties: AccordionProps): React.JSX.Element {
  return <Accordion disableGutters elevation={0} {...properties} />;
}

export function StudioAccordionSummary(properties: AccordionSummaryProps): React.JSX.Element {
  return <AccordionSummary {...properties} />;
}

export function StudioAccordionDetails(properties: AccordionDetailsProps): React.JSX.Element {
  return <AccordionDetails {...properties} />;
}

export function StudioToggleButton(properties: ToggleButtonProps): React.JSX.Element {
  return <ToggleButton size="small" {...properties} />;
}

export function StudioToggleButtonGroup({
  onKeyDown,
  ...properties
}: ToggleButtonGroupProps): React.JSX.Element {
  function handleKeyDown(event: KeyboardEvent<HTMLDivElement>): void {
    onKeyDown?.(event);
    if (event.defaultPrevented || (event.key !== "ArrowLeft" && event.key !== "ArrowRight")) {
      return;
    }
    const buttons = [
      ...event.currentTarget.querySelectorAll<HTMLButtonElement>("button:not(:disabled)"),
    ];
    const target = event.target instanceof Element ? event.target.closest("button") : null;
    const currentIndex = target === null ? -1 : buttons.indexOf(target);
    if (currentIndex < 0) {
      return;
    }
    event.preventDefault();
    const direction = event.key === "ArrowRight" ? 1 : -1;
    const next = buttons[(currentIndex + direction + buttons.length) % buttons.length];
    next?.focus();
    next?.click();
  }

  return <ToggleButtonGroup exclusive size="small" {...properties} onKeyDown={handleKeyDown} />;
}

export function StudioTooltip(properties: TooltipProps): React.JSX.Element {
  const popper =
    typeof properties.slotProps?.popper === "object" ? properties.slotProps.popper : {};
  return (
    <Tooltip
      arrow
      enterDelay={450}
      {...properties}
      slotProps={{
        ...properties.slotProps,
        popper: { disablePortal: true, ...popper },
      }}
    />
  );
}
