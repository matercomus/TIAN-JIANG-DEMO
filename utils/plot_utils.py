import plotly.graph_objects as go
import plotly.express as px
import polars as pl


def plot_histogram_pl(df: pl.DataFrame, col: str, title: str = None):
    data = df[col].to_list()
    fig = go.Figure(data=[go.Histogram(x=data)])
    fig.update_layout(title=title or f"Histogram of {col}", xaxis_title=col, yaxis_title="Count")
    fig.show()


def plot_scatter_pl(df: pl.DataFrame, x: str, y: str, color: str = None, hover: str = None, title: str = None):
    x_data = df[x].to_list()
    y_data = df[y].to_list()
    hover_data = df[hover].to_list() if hover and hover in df.columns else None

    if color and color in df.columns:
        color_data = df[color].to_list()
        # If color is categorical, map to colors
        if isinstance(color_data[0], str) or isinstance(color_data[0], (int, float)) and len(set(color_data)) < 20:
            unique_cats = list(sorted(set(color_data)))
            palette = px.colors.qualitative.Plotly
            color_map = {cat: palette[i % len(palette)] for i, cat in enumerate(unique_cats)}
            marker_colors = [color_map[val] for val in color_data]
            text = hover_data if hover_data else None
            fig = go.Figure(data=[go.Scatter(
                x=x_data,
                y=y_data,
                mode="markers",
                marker=dict(color=marker_colors),
                text=text,
                customdata=color_data,
                hovertemplate=f"{x}: %{{x}}<br>{y}: %{{y}}<br>{color}: %{{customdata}}<extra></extra>"
            )])
            # Add legend manually
            for cat, colr in color_map.items():
                fig.add_trace(go.Scatter(
                    x=[None], y=[None],
                    mode='markers',
                    marker=dict(size=10, color=colr),
                    legendgroup=str(cat),
                    showlegend=True,
                    name=str(cat)
                ))
        else:
            # If color is numeric, pass directly
            fig = go.Figure(data=[go.Scatter(
                x=x_data,
                y=y_data,
                mode="markers",
                marker=dict(color=color_data, colorbar=dict(title=color), colorscale='Viridis'),
                text=hover_data
            )])
    else:
        fig = go.Figure(data=[go.Scatter(
            x=x_data,
            y=y_data,
            mode="markers",
            text=hover_data
        )])
    fig.update_layout(title=title or f"Scatter: {x} vs {y}", xaxis_title=x, yaxis_title=y)
    fig.show()


def plot_bar_pl(df: pl.DataFrame, x: str, y: str, title: str = None):
    fig = go.Figure(data=[go.Bar(x=df[x].to_list(), y=df[y].to_list())])
    fig.update_layout(title=title or f"Bar plot: {x} vs {y}", xaxis_title=x, yaxis_title=y)
    fig.show()


def plot_time_series_pl(df: pl.DataFrame, x: str, y: str, title: str = None):
    fig = go.Figure(data=[go.Scatter(x=df[x].to_list(), y=df[y].to_list(), mode="lines+markers")])
    fig.update_layout(title=title or f"Time Series: {y} over {x}", xaxis_title=x, yaxis_title=y)
    fig.show() 