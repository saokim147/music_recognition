from __future__ import print_function
from data.dataset import Dataset
from torch.utils import data
from models.focal_loss import FocalLoss
from models.metrics import *
from models.model_registry import get_model
from utils.visualizer import Visualizer
import time
import hydra
from omegaconf import DictConfig, OmegaConf
from config.config_schema import Config
from torch.nn import DataParallel
from torch.optim.lr_scheduler import StepLR
from val import *

torch.manual_seed(3407)


def save_model(model, save_path, name, iter_cnt):
    os.makedirs(save_path, exist_ok=True)
    save_name = os.path.join(save_path, name + '_' + str(iter_cnt) + '.pth')
    torch.save(model.state_dict(), save_name)
    return save_name


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: Config) -> None:
    """Main training function with Hydra configuration.

    Args:
        cfg: Hydra configuration object with type hints from Config schema
    """

    device = torch.device('cuda' if cfg.training.use_gpu else 'cpu')
    train_dataset = Dataset(cfg.paths.train_root, cfg.paths.train_list, phase='train',
                            input_shape=tuple(cfg.data.input_shape),
                            mp3aug_ratio=cfg.data.mp3aug_ratio, npy_aug=cfg.data.npy_aug)
    trainloader = data.DataLoader(train_dataset,
                                  batch_size=cfg.data.train_batch_size,
                                  shuffle=True,
                                  num_workers=cfg.data.num_workers)
    print('{} train iters per epoch:'.format(len(trainloader)))

    if cfg.loss.type == 'focal_loss':
        criterion = FocalLoss(gamma=2)
    else:
        criterion = torch.nn.CrossEntropyLoss()

    model = get_model(cfg.model.backbone)

    if cfg.metric.type == 'add_margin':
        metric_fc = AddMarginProduct(512, cfg.model.num_classes, s=30, m=0.35)
    elif cfg.metric.type == 'arc_margin':
        metric_fc = ArcMarginProduct(512, cfg.model.num_classes, s=30, m=0.5, easy_margin=cfg.metric.easy_margin)
    elif cfg.metric.type == 'sphere':
        metric_fc = SphereProduct(512, cfg.model.num_classes, m=4)
    else:
        metric_fc = nn.Linear(512, cfg.model.num_classes)

    model.to(device)
    model = DataParallel(model)
    metric_fc.to(device)
    metric_fc = DataParallel(metric_fc)

    if cfg.optimizer.type == 'sgd':
        optimizer = torch.optim.SGD([{'params': model.parameters()}, {'params': metric_fc.parameters()}],
                                    lr=cfg.optimizer.lr, weight_decay=cfg.optimizer.weight_decay)
    else:
        optimizer = torch.optim.Adam([{'params': model.parameters()}, {'params': metric_fc.parameters()}],
                                     lr=cfg.optimizer.lr, weight_decay=cfg.optimizer.weight_decay)
    scheduler = StepLR(optimizer, step_size=cfg.scheduler.lr_step, gamma=0.1)

    start = time.time()
    mrr_best = 0
    inference_times = []
    for i in range(1, cfg.training.max_epoch + 1):
        model.train()
        for ii, data in enumerate(trainloader):
            data_input, label = data
            data_input = data_input.to(device)
            label = label.to(device).long()
            feature = model(data_input)
            output = metric_fc(feature, label)
            loss = criterion(output, label)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            iters = i * len(trainloader) + ii

            if iters % cfg.training.print_freq == 0:
                output = output.data.cpu().numpy()
                output = np.argmax(output, axis=1)
                label = label.data.cpu().numpy()
                acc = np.mean((output == label).astype(int))
                speed = cfg.training.print_freq / (time.time() - start)
                time_str = time.asctime(time.localtime(time.time()))
                print('{} train epoch {} iter {} {} iters/s loss {} acc {}'.format(time_str, i, ii, speed, loss.item(), acc))
                start = time.time()

        if i % cfg.training.save_interval == 0 or i == cfg.training.max_epoch:
            print('calculating mrr .....')
            save_model(model, cfg.paths.checkpoints_path, cfg.model.backbone, 'latest')
            model.eval()
            data_val = read_val(cfg.paths.val_list, cfg.paths.train_root)
            inference_start = time.time()
            mrr = mrr_score(model, data_val, tuple(cfg.data.input_shape))
            inference_time = time.time() - inference_start
            inference_times.append(inference_time)
            print(f'epoch {i}: MRR= {mrr}, inference time= {inference_time:.2f}s')
            if mrr > mrr_best:
                mrr_best = mrr
                save_model(model, cfg.paths.checkpoints_path, cfg.model.backbone, 'best')

        scheduler.step()

    # Print average inference time
    if inference_times:
        avg_inference_time = sum(inference_times) / len(inference_times)
        print(f'\n=== Training Complete ===')
        print(f'Total validation runs: {len(inference_times)}')
        print(f'Average inference time: {avg_inference_time:.2f}s')
        print(f'Best MRR: {mrr_best:.4f}')


if __name__ == '__main__':
    main()
