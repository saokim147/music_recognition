"""Typed configuration schema for Hydra."""
from dataclasses import dataclass, field
from typing import Tuple


@dataclass
class ModelConfig:
    """Model architecture configuration."""
    backbone: str = 'resnet34'
    classify: str = 'softmax'
    num_classes: int = 5000
    use_se: bool = False


@dataclass
class MetricConfig:
    """Metric learning configuration."""
    type: str = 'arc_margin'  # Options: 'arc_margin', 'add_margin', 'sphere', None
    easy_margin: bool = False


@dataclass
class LossConfig:
    """Loss function configuration."""
    type: str = 'focal_loss'  # Options: 'focal_loss', 'cross_entropy'


@dataclass
class PathsConfig:
    """Data paths configuration."""
    meta_train: str = '/content/music_recognition/preprocessed/train_meta.csv'
    train_root: str = '/content/music_recognition/preprocessed'
    train_list: str = '/content/music_recognition/full_data_train.txt'
    val_list: str = '/content/music_recognition/full_data_val.txt'
    checkpoints_path: str = '/content/music_recognition/checkpoints'
    debug_file: str = '/tmp/debug'
    result_file: str = '/result/submission.csv'


@dataclass
class DataConfig:
    """Data processing configuration."""
    input_shape: Tuple[int, int] = (630, 80)
    train_batch_size: int = 32
    num_workers: int = 0
    mp3aug_ratio: float = 1.0
    npy_aug: bool = True


@dataclass
class OptimizerConfig:
    """Optimizer configuration."""
    type: str = 'sgd'  # Options: 'sgd', 'adam'
    lr: float = 0.01
    weight_decay: float = 0.1


@dataclass
class SchedulerConfig:
    """Learning rate scheduler configuration."""
    lr_step: int = 10
    lr_decay: float = 0.5


@dataclass
class TrainingConfig:
    """Training configuration."""
    max_epoch: int = 100
    print_freq: int = 100
    save_interval: int = 1
    use_gpu: bool = True
    gpu_id: str = "0, 1"


@dataclass
class Config:
    """Main configuration class for music recognition training."""
    env: str = 'default'
    display: bool = False
    finetune: bool = False

    model: ModelConfig = field(default_factory=ModelConfig)
    metric: MetricConfig = field(default_factory=MetricConfig)
    loss: LossConfig = field(default_factory=LossConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    data: DataConfig = field(default_factory=DataConfig)
    optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
