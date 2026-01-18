import torch
import torch.nn as nn
import torchvision.models as models


class VGGAudio(nn.Module):
    """VGG wrapper for single-channel audio input with 512-dim embedding output."""

    def __init__(self, vgg_model):
        super(VGGAudio, self).__init__()
        # Modify first conv layer: 3 channels -> 1 channel
        original_conv = vgg_model.features[0]
        self.features = vgg_model.features
        self.features[0] = nn.Conv2d(
            1, original_conv.out_channels,
            kernel_size=original_conv.kernel_size,
            stride=original_conv.stride,
            padding=original_conv.padding
        )
        # Adaptive pooling to handle variable input sizes
        self.avgpool = nn.AdaptiveAvgPool2d((7, 7))
        # Replace classifier with 512-dim embedding
        self.classifier = nn.Sequential(
            nn.Linear(512 * 7 * 7, 4096),
            nn.ReLU(True),
            nn.Dropout(),
            nn.Linear(4096, 512),
        )
        self.bn = nn.BatchNorm1d(512)

        # Initialize new layers
        nn.init.kaiming_normal_(self.features[0].weight, mode='fan_out', nonlinearity='relu')
        nn.init.xavier_normal_(self.classifier[3].weight)
        nn.init.constant_(self.classifier[3].bias, 0)

    def forward(self, x):
        x = self.features(x)
        x = self.avgpool(x)
        x = x.view(x.size(0), -1)
        x = self.classifier(x)
        if x.shape[0] > 1:
            x = self.bn(x)
        return x


def vgg16_audio():
    """VGG16 adapted for single-channel audio with 512-dim output."""
    return VGGAudio(models.vgg16(weights=None))


def vgg19_audio():
    """VGG19 adapted for single-channel audio with 512-dim output."""
    return VGGAudio(models.vgg19(weights=None))
