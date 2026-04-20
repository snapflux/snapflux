import { DiscoveryModule } from '@golevelup/nestjs-discovery';
import { DynamicModule, Global, Module, Provider, Type } from '@nestjs/common';
import { SNAPFLUX_OPTIONS } from './snapflux.constants';
import { SnapfluxService } from './snapflux.service';
import type { SnapfluxModuleAsyncOptions, SnapfluxOptions, SnapfluxOptionsFactory } from './snapflux.types';

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [SnapfluxService],
  exports: [SnapfluxService],
})
export class SnapfluxModule {
  static register(options: SnapfluxOptions): DynamicModule {
    return {
      global: true,
      module: SnapfluxModule,
      imports: [DiscoveryModule],
      providers: [{ provide: SNAPFLUX_OPTIONS, useValue: options }, SnapfluxService],
      exports: [SnapfluxService],
    };
  }

  static registerAsync(options: SnapfluxModuleAsyncOptions): DynamicModule {
    return {
      global: true,
      module: SnapfluxModule,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [...SnapfluxModule.createAsyncProviders(options), SnapfluxService],
      exports: [SnapfluxService],
    };
  }

  private static createAsyncProviders(options: SnapfluxModuleAsyncOptions): Provider[] {
    if (options.useFactory) {
      return [
        {
          provide: SNAPFLUX_OPTIONS,
          useFactory: options.useFactory,
          inject: (options.inject as any[]) ?? [],
        },
      ];
    }

    const useClass = (options.useClass ?? options.useExisting) as Type<SnapfluxOptionsFactory>;
    const providers: Provider[] = [
      {
        provide: SNAPFLUX_OPTIONS,
        useFactory: async (factory: SnapfluxOptionsFactory) => factory.createOptions(),
        inject: [useClass],
      },
    ];

    if (options.useClass) {
      providers.push({ provide: useClass, useClass });
    }

    return providers;
  }
}
