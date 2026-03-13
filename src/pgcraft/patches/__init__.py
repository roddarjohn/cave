from pgcraft.patches import view_render


def apply_all() -> None:
    """Apply all monkey-patches."""
    view_render.apply()
