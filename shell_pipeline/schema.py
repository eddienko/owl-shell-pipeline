from pathlib import Path

import voluptuous as vo

schema = vo.Schema(
    vo.All(
        {
            vo.Required("version"): vo.In(['2.0']),
            vo.Optional("output_path"): vo.Coerce(Path),
            vo.Optional("input_path"): vo.Coerce(Path),
            vo.Required("command"): str,
            vo.Optional("use_dask", default=False): bool,
        },
    )
)
